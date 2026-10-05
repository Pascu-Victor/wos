#include "wkictl/vfs.hpp"

#include <abi-bits/access.h>
#include <sys/process.h>
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
#include "wkictl/target.hpp"

namespace {

using wkictl::route_name;

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
