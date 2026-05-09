#include <fcntl.h>
#include <glob.h>
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
#include <string>

namespace {

auto command_basename(const char* path) -> const char* {
    if (path == nullptr) {
        return "";
    }
    const char* slash = std::strrchr(path, '/');
    return slash == nullptr ? path : slash + 1;
}

auto usage() -> int {
    std::println(stderr,
                 "usage:\n  locally <command> [args...]\n  remotely <command> [args...]\n  homeward <command> [args...]\n  on <hostname> "
                 "<command> [args...]\n  forward "
                 "[+include_path] [-exclude_path] [--] <command> [args...]\n  wosid\n  wkictl "
                 "target <show|clear|set>\n  wkictl vfs <list|defaults|clear|add|probe>\n  wkictl perf <show>\n  wkictl wosid");
    return 1;
}

auto exec_command(char** argv) -> int {
    execvp(argv[0], argv);
    std::println(stderr, "{}: exec failed: {}", argv[0], std::strerror(errno));
    return 127;
}

auto read_trimmed_file(const char* path, char* out, size_t out_size) -> bool {
    if (path == nullptr || out == nullptr || out_size == 0) {
        return false;
    }

    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        out[0] = '\0';
        return false;
    }

    ssize_t n = read(fd, out, out_size - 1);
    int saved_errno = errno;
    close(fd);
    errno = saved_errno;
    if (n <= 0) {
        out[0] = '\0';
        return false;
    }

    while (n > 0 && (out[n - 1] == '\n' || out[n - 1] == '\r')) {
        --n;
    }
    out[n] = '\0';
    return true;
}

auto print_wosid() -> int {
    std::array<char, 64> launcher = {};
    std::array<char, 64> runner = {};
    std::array<char, 32> remote_pid = {};

    const char* launcher_text =
        read_trimmed_file("/proc/self/wki_launcher", launcher.data(), launcher.size()) ? launcher.data() : "<unknown>";
    const char* runner_text = read_trimmed_file("/proc/self/wki_runner", runner.data(), runner.size()) ? runner.data() : "<unknown>";
    const char* remote_pid_text =
        read_trimmed_file("/proc/self/wki_remote_pid", remote_pid.data(), remote_pid.size()) ? remote_pid.data() : "0";

    std::println("spawner={} host={} pid={} remote_pid={}", launcher_text, runner_text, ker::process::getpid(), remote_pid_text);
    return 0;
}

auto route_name(uint32_t route) -> const char*;

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
    int64_t launcher_len = ker::process::wki_launcher_node(launcher.data(), launcher.size());
    if (launcher_len <= 0 || launcher[0] == '\0') {
        std::println(stderr, "homeward: failed to resolve launcher node");
        return 1;
    }

    int64_t rc = ker::process::setwkitarget(launcher.data(), static_cast<uint64_t>(launcher_len), ker::process::WKI_TARGET_FLAG_STRICT);
    if (rc < 0) {
        std::println(stderr, "homeward: failed to target launcher '{}': {}", launcher.data(), static_cast<long>(rc));
        return 1;
    }
    return exec_command(argv + 1);
}

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
    int glob_rc = glob(operand, 0, nullptr, &matches);
    if (glob_rc != 0) {
        globfree(&matches);
        std::println(stderr, "forward: no matches for '{}'", operand);
        return false;
    }

    bool ok = true;
    for (size_t i = 0; i < matches.gl_pathc; ++i) {
        if (!add_forward_rule(matches.gl_pathv[i], route)) {
            ok = false;
            break;
        }
    }
    globfree(&matches);
    return ok;
}

auto run_forward(int argc, char** argv) -> int {
    int command_index = 1;
    for (; command_index < argc; ++command_index) {
        const char* arg = argv[command_index];
        if (std::strcmp(arg, "--") == 0) {
            command_index++;
            break;
        }
        if ((arg[0] != '+' && arg[0] != '-') || arg[1] == '\0') {
            break;
        }

        uint32_t route = arg[0] == '+' ? ker::abi::vfs::WKI_VFS_ROUTE_HOST : ker::abi::vfs::WKI_VFS_ROUTE_LOCAL;
        if (!add_forward_operand(arg + 1, route)) {
            return 1;
        }
    }

    if (command_index >= argc) {
        return usage();
    }
    return exec_command(argv + command_index);
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

auto flags_name(uint32_t flags, char* out, size_t out_size) -> const char* {
    if (out == nullptr || out_size == 0) {
        return "";
    }
    out[0] = '\0';
    auto append = [&](const char* text) {
        size_t used = std::strlen(out);
        if (used >= out_size - 1) {
            return;
        }
        if (out[0] != '\0') {
            std::strncat(out, "|", out_size - used - 1);
            used = std::strlen(out);
            if (used >= out_size - 1) {
                return;
            }
        }
        std::strncat(out, text, out_size - used - 1);
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
    if (out[0] == '\0') {
        std::strncpy(out, "auto", out_size - 1);
        out[out_size - 1] = '\0';
    }
    return out;
}

auto print_target() -> int {
    char hostname[64] = {};
    uint32_t flags = 0;
    int64_t rc = ker::process::getwkitarget(hostname, sizeof(hostname), &flags);
    if (rc < 0) {
        std::println(stderr, "wkictl: target get failed: {}", static_cast<long>(rc));
        return 1;
    }
    char flags_buf[64] = {};
    if (rc == 0) {
        std::println("target: host=<auto> flags={}", flags_name(flags, flags_buf, sizeof(flags_buf)));
    } else {
        std::println("target: host={} flags={}", hostname, flags_name(flags, flags_buf, sizeof(flags_buf)));
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

auto list_rules(bool defaults) -> int {
    bool any = false;
    for (uint32_t i = 0;; ++i) {
        char prefix[128] = {};
        uint32_t route = 0;
        int rc = defaults ? ker::abi::vfs::wki_rule_get_default_vfs(i, prefix, sizeof(prefix), &route)
                          : ker::abi::vfs::wki_rule_get_vfs(i, prefix, sizeof(prefix), &route);
        if (rc == -ENOENT) {
            break;
        }
        if (rc < 0) {
            std::println(stderr, "wkictl: vfs rule get failed at {}: {}", i, rc);
            return 1;
        }
        std::println("vfs-{}[{}]: {} -> {}", defaults ? "default" : "task", i, prefix, route_name(route));
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

auto handle_perf(int argc, char** argv) -> int {
    if (argc >= 3 && std::strcmp(argv[2], "show") != 0) {
        return usage();
    }
    std::println("perf: WKI placement is observable through journal logs, /proc/*/wki_runner, and the perf WKI event scopes.");
    return 0;
}

auto run_wkictl(int argc, char** argv) -> int {
    if (argc < 2) {
        return usage();
    }
    if (std::strcmp(argv[1], "wosid") == 0) {
        return print_wosid();
    }
    if (std::strcmp(argv[1], "target") == 0) {
        return handle_target(argc, argv);
    }
    if (std::strcmp(argv[1], "vfs") == 0) {
        return handle_vfs(argc, argv);
    }
    if (std::strcmp(argv[1], "perf") == 0) {
        return handle_perf(argc, argv);
    }
    return usage();
}

}  // namespace

auto main(int argc, char** argv) -> int {
    const char* name = command_basename(argc > 0 ? argv[0] : "wkictl");
    if (std::strcmp(name, "locally") == 0) {
        return run_locally(argc, argv);
    }
    if (std::strcmp(name, "remotely") == 0) {
        return run_remotely(argc, argv);
    }
    if (std::strcmp(name, "homeward") == 0) {
        return run_homeward(argc, argv);
    }
    if (std::strcmp(name, "on") == 0) {
        return run_on(argc, argv);
    }
    if (std::strcmp(name, "forward") == 0) {
        return run_forward(argc, argv);
    }
    if (std::strcmp(name, "wosid") == 0) {
        return print_wosid();
    }
    return run_wkictl(argc, argv);
}
