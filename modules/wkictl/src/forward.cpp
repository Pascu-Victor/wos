#include <glob.h>
#include <sys/process.h>
#include <sys/vfs.h>

#include <cstdint>
#include <cstdio>
#include <cstring>

#include "wkictl/cli.hpp"
#include "wkictl/target.hpp"
#include "wkictl/vfs.hpp"

namespace wkictl {

auto route_name(uint32_t route) -> const char* {
    switch (route) {
        case ker::abi::vfs::WKI_VFS_ROUTE_LOCAL:
            return "local";
        case ker::abi::vfs::WKI_VFS_ROUTE_HOST:
            return "host";
        default:
            return "unknown";
    }
}

}  // namespace wkictl

namespace {

using wkictl::route_name;

auto has_glob_meta(const char* text) -> bool {
    if (text == nullptr) {
        return false;
    }
    for (const char* p = text; *p != '\0'; ++p) {
        if (*p == '*' || *p == '?' || *p == '[') {
            return true;
        }
    }
    return false;
}

auto add_forward_rule(const char* path, uint32_t route) -> bool {
    int rc = ker::abi::vfs::wki_rule_add_vfs(path, route);
    if (rc < 0) {
        std::fprintf(stderr, "forward: failed to add %s rule for '%s': %d\n", route_name(route), path, rc);
        return false;
    }
    return true;
}

auto add_forward_operand(const char* operand, uint32_t route) -> bool {
    if (operand == nullptr || operand[0] == '\0') {
        std::fputs("forward: empty VFS rule operand\n", stderr);
        return false;
    }

    if (!has_glob_meta(operand)) {
        return add_forward_rule(operand, route);
    }

    glob_t matches{};
    int const GLOB_RC = glob(operand, 0, nullptr, &matches);
    if (GLOB_RC != 0) {
        globfree(&matches);
        std::fprintf(stderr, "forward: no matches for '%s'\n", operand);
        return false;
    }

    bool ok = true;
    for (std::size_t i = 0; i < matches.gl_pathc; ++i) {
        if (!add_forward_rule(matches.gl_pathv[i], route)) {
            ok = false;
            break;
        }
    }
    globfree(&matches);
    return ok;
}

}  // namespace

namespace wkictl {

auto run_forward(int argc, char** argv) -> int {
    int command_index = 1;
    const char* target_policy = nullptr;
    bool one_shot = false;
    while (command_index < argc) {
        if (std::strcmp(argv[command_index], "--clear") == 0) {
            int const CLEAR_RC = ker::abi::vfs::wki_rule_clear_vfs();
            if (CLEAR_RC < 0) {
                std::fprintf(stderr, "forward: failed to clear inherited VFS rules: %d\n", CLEAR_RC);
                return 1;
            }
            command_index++;
            continue;
        }
        if (std::strcmp(argv[command_index], "--target") == 0) {
            if (++command_index >= argc) {
                std::fputs("forward: --target requires a placement policy\n", stderr);
                return 1;
            }
            target_policy = argv[command_index++];
            continue;
        }
        if (std::strcmp(argv[command_index], "--one-shot") == 0) {
            one_shot = true;
            command_index++;
            continue;
        }
        break;
    }

    if (one_shot && target_policy == nullptr) {
        std::fputs("forward: --one-shot requires --target\n", stderr);
        return 1;
    }

    for (; command_index < argc; ++command_index) {
        const char* arg = argv[command_index];
        if (std::strcmp(arg, "--") == 0) {
            command_index++;
            break;
        }
        if ((arg[0] != '+' && arg[0] != '-') || arg[1] == '\0') {
            break;
        }

        uint32_t const ROUTE = arg[0] == '+' ? ker::abi::vfs::WKI_VFS_ROUTE_HOST : ker::abi::vfs::WKI_VFS_ROUTE_LOCAL;
        if (!add_forward_operand(arg + 1, ROUTE)) {
            return 1;
        }
    }

    if (target_policy != nullptr) {
        uint32_t const EXTRA_FLAGS = one_shot ? ker::process::WKI_TARGET_FLAG_ONESHOT : 0;
        int64_t const TARGET_RC = set_target_policy(target_policy, EXTRA_FLAGS);
        if (TARGET_RC < 0) {
            std::fprintf(stderr, "forward: failed to set '%s' placement policy: %ld\n", target_policy, static_cast<long>(TARGET_RC));
            return 1;
        }
    }

    if (command_index >= argc) {
        return usage();
    }
    return exec_command(argv + command_index);
}

}  // namespace wkictl
