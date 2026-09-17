#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <string.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

#include <initializer_list>

namespace {
bool provision(const char* base, bool remove) {
    char path[512], private_path[512], public_path[512];
    snprintf(private_path, sizeof(private_path), "%s/private", base);
    snprintf(public_path, sizeof(public_path), "%s/public", base);
    const char* names[] = {"relative-link", "absolute-link", "public-link", "absolute-public", "loop", "escape"};
    if (remove) {
        bool ok = true;
        for (auto name : names) {
            snprintf(path, sizeof(path), "%s/%s", base, name);
            ok &= unlink(path) == 0 || errno == ENOENT;
        }
        ok &= unlink(public_path) == 0 || errno == ENOENT;
        ok &= rmdir(private_path) == 0 || errno == ENOENT;
        ok &= rmdir(base) == 0 || errno == ENOENT;
        return ok;
    }
    if (mkdir(base, 0755) || chmod(base, 0755) || mkdir(private_path, 0710) || chown(private_path, 0, 2000) || chmod(private_path, 0710))
        return false;
    int fd = open(public_path, O_CREAT | O_EXCL | O_WRONLY, 0600);
    if (fd < 0) return false;
    if (close(fd) || chmod(public_path, 0000)) return false;
    // Mapped links use the publishing process namespace, independently of
    // the physical mount path after pivot_root.
    char absolute_private[512], absolute_public[512];
    snprintf(absolute_private, sizeof(absolute_private), "%s/private/../public", base);
    snprintf(absolute_public, sizeof(absolute_public), "%s/public", base);
    const char* targets[] = {"private/../public", absolute_private, "public", absolute_public, "loop", "../../outside"};
    for (unsigned i = 0; i < 6; ++i) {
        snprintf(path, sizeof(path), "%s/%s", base, names[i]);
        if (symlink(targets[i], path)) return false;
    }
    return true;
}
bool check(const char* remote, const char* alias, bool grouped, bool off) {
    struct Case {
        const char* tail;
        int ungrouped;
        int group;
        const char* canonical;
    };
    const Case cases[] = {{"public", 0, 0, "public"},
                          {"private/../public", EACCES, 0, "public"},
                          {"private/.", EACCES, 0, "private"},
                          {"private/missing", EACCES, ENOENT, ""},
                          {"relative-link", EACCES, 0, "public"},
                          {"absolute-link", EACCES, 0, "public"},
                          {"public-link", 0, 0, "public"},
                          {"absolute-public", 0, 0, "public"},
                          {".", 0, 0, ""},
                          {"private", 0, 0, "private"},
                          {"public/", ENOTDIR, ENOTDIR, ""},
                          {"private/../public/", EACCES, ENOTDIR, ""},
                          {"public/..", ENOTDIR, ENOTDIR, ""},
                          {"loop", ELOOP, ELOOP, ""},
                          {"missing", ENOENT, ENOENT, ""},
                          {"escape", EPERM, EPERM, ""},
                          {"private//../public", EACCES, 0, "public"},
                          {"public/.", ENOTDIR, ENOTDIR, ""},
                          {"private/..", EACCES, 0, ""},
                          {"./public", 0, 0, "public"},
                          {"", 0, 0, ""},
                          {"private/../.", EACCES, 0, ""},
                          {"private//.", EACCES, 0, "private"},
                          {"../../outside", EPERM, EPERM, ""}};
    unsigned total = 0, passed = 0;
    for (const char* prefix : {remote, alias})
        for (const auto& c : cases) {
            char path[512], canonical[512];
            snprintf(path, sizeof(path), "%s/%s", prefix, c.tail);
            snprintf(canonical, sizeof(canonical), "%s%s%s", remote, *c.canonical ? "/" : "", c.canonical);
            int normal = off ? ENOTSUP : grouped ? c.group : c.ungrouped;
            for (unsigned mode = 0; mode < 4; ++mode) {
                size_t capacity = mode == 0 ? 512 : mode == 1 ? 1 : mode == 2 ? 0 : strlen(canonical) + 1;
                int expected = mode == 2 ? EINVAL : normal ? normal : mode == 1 ? ERANGE : 0;
                char output[512];
                memset(output, 'Z', sizeof(output));
                int result = ker::abi::vfs::realpath(path, output, capacity);
                bool unchanged = true;
                if (expected)
                    for (char b : output) unchanged &= b == 'Z';
                bool ok = result == -expected && unchanged;
                if (!expected && result == 0) ok &= strcmp(output, canonical) == 0;
                ++total;
                passed += ok;
                if (!ok)
                    printf("FAIL group=%d alias=%d path=%s mode=%u result=%d expected=%d unchanged=%d\n", grouped, prefix == alias, c.tail,
                           mode, result, -expected, unchanged);
            }
        }
    printf("profile grouped=%d passed=%u total=%u\n", grouped, passed, total);
    return passed == total;
}
}  // namespace
int main(int argc, char** argv) {
    if (argc < 3 || geteuid() != 0) return 2;
    if (strcmp(argv[1], "check")) {
        if (argc != 3 || ker::abi::vfs::wki_rule_add_vfs("/", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL)) return 2;
        bool remove = strcmp(argv[1], "cleanup") == 0;
        if (!remove && strcmp(argv[1], "setup")) return 2;
        bool ok = provision(argv[2], remove);
        printf("%s %s\n", ok ? "PASS" : "FAIL", argv[1]);
        return ok ? 0 : 1;
    }
    if (argc != 5 || (strcmp(argv[4], "on") && strcmp(argv[4], "off"))) return 2;
    unsigned passed = 0;
    for (bool grouped : {false, true}) {
        pid_t pid = fork();
        if (pid == 0) {
            gid_t group = 2000;
            if (setgroups(grouped ? 1 : 0, grouped ? &group : nullptr) || setgid(2001) || setuid(1000)) _exit(2);
            using Control = ker::abi::process::CredentialControl;
            if (ker::process::prctl(static_cast<int>(Control::SET_NO_NEW_PRIVILEGES), 1, 0, 0, 0) ||
                ker::process::prctl(static_cast<int>(Control::DROP_AUTHORITY_CEILING), 0, 0, 0, 0))
                _exit(2);
            bool ok = check(argv[2], argv[3], grouped, strcmp(argv[4], "off") == 0);
            fflush(stdout);
            _exit(ok ? 0 : 1);
        }
        int status = 0;
        if (pid > 0 && waitpid(pid, &status, 0) == pid && WIFEXITED(status) && WEXITSTATUS(status) == 0) ++passed;
    }
    printf("%s remote REALPATH checks=384\n", passed == 2 ? "PASS" : "FAIL");
    return passed == 2 ? 0 : 1;
}
