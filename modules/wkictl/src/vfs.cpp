#include "wkictl/vfs.hpp"

#include <abi-bits/access.h>
#include <glob.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <print>

#include "wkictl/cli.hpp"

namespace {

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

auto parse_route(const char* text, uint32_t* out) -> bool {
    if (text == nullptr || out == nullptr) {
        return false;
    }
    if (std::strcmp(text, "local") == 0) {
        *out = ker::abi::vfs::WKI_VFS_ROUTE_LOCAL;
        return true;
    }
    if (std::strcmp(text, "host") == 0) {
        *out = ker::abi::vfs::WKI_VFS_ROUTE_HOST;
        return true;
    }
    return false;
}

auto add_forward_rule(const char* path, uint32_t route) -> bool {
    int rc = ker::abi::vfs::wki_rule_add_vfs(path, route);
    if (rc < 0) {
        std::println(stderr, "forward: failed to add {} rule for '{}': {}", route_name(route), path, rc);
        return false;
    }
    return true;
}

auto add_forward_operand(const char* operand, uint32_t route) -> bool {
    if (operand == nullptr || operand[0] == '\0') {
        std::println(stderr, "forward: empty VFS rule operand");
        return false;
    }

    if (!has_glob_meta(operand)) {
        return add_forward_rule(operand, route);
    }

    glob_t matches{};
    int const GLOB_RC = glob(operand, 0, nullptr, &matches);
    if (GLOB_RC != 0) {
        globfree(&matches);
        std::println(stderr, "forward: no matches for '{}'", operand);
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

auto list_rules(bool defaults) -> int {
    bool any = false;
    for (uint32_t i = 0;; ++i) {
        std::array<char, 128> prefix = {};
        uint32_t route = 0;
        int rc = defaults ? ker::abi::vfs::wki_rule_get_default_vfs(i, prefix.data(), prefix.size(), &route)
                          : ker::abi::vfs::wki_rule_get_vfs(i, prefix.data(), prefix.size(), &route);
        if (rc == -ENOENT) {
            break;
        }
        if (rc < 0) {
            std::println(stderr, "wkictl: vfs rule get failed at {}: {}", i, rc);
            return 1;
        }
        std::println("vfs-{}[{}]: {} -> {}", defaults ? "default" : "task", i, prefix.data(), route_name(route));
        any = true;
    }
    if (!any) {
        std::println("vfs-{}: no rules", defaults ? "default" : "task");
    }
    return 0;
}

auto probe_path(const char* path) -> int {
    struct stat st{};
    errno = 0;
    int stat_rc = stat(path, &st);
    std::println("probe: stat {} => {} errno={}", path, stat_rc, errno);
    if (stat_rc == 0) {
        std::println("probe: mode=0{:o} size={} ino={}", st.st_mode, static_cast<long long>(st.st_size), static_cast<long long>(st.st_ino));
    }
    errno = 0;
    int access_rc = access(path, F_OK);
    std::println("probe: access(F_OK) => {} errno={}", access_rc, errno);
    return stat_rc == 0 || access_rc == 0 ? 0 : 1;
}

}  // namespace

namespace wkictl {

auto run_forward(int argc, char** argv) -> int {
    int command_index = 1;
    if (command_index < argc && std::strcmp(argv[command_index], "--clear") == 0) {
        int const CLEAR_RC = ker::abi::vfs::wki_rule_clear_vfs();
        if (CLEAR_RC < 0) {
            std::println(stderr, "forward: failed to clear inherited VFS rules: {}", CLEAR_RC);
            return 1;
        }
        command_index++;
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

    if (command_index >= argc) {
        return usage();
    }
    return exec_command(argv + command_index);
}

auto handle_vfs(int argc, char** argv) -> int {
    if (argc < 3 || std::strcmp(argv[2], "list") == 0) {
        return list_rules(false);
    }
    if (std::strcmp(argv[2], "defaults") == 0) {
        return list_rules(true);
    }
    if (std::strcmp(argv[2], "clear") == 0) {
        int rc = ker::abi::vfs::wki_rule_clear_vfs();
        if (rc < 0) {
            std::println(stderr, "wkictl: vfs clear failed: {}", rc);
            return 1;
        }
        return list_rules(false);
    }
    if (std::strcmp(argv[2], "add") == 0) {
        if (argc < 5) {
            return usage();
        }
        uint32_t route = 0;
        if (!parse_route(argv[4], &route)) {
            return usage();
        }
        int rc = ker::abi::vfs::wki_rule_add_vfs(argv[3], route);
        if (rc < 0) {
            std::println(stderr, "wkictl: vfs add failed: {}", rc);
            return 1;
        }
        return list_rules(false);
    }
    if (std::strcmp(argv[2], "probe") == 0) {
        if (argc < 4) {
            return usage();
        }
        return probe_path(argv[3]);
    }
    return usage();
}

}  // namespace wkictl
