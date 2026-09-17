#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

#include <initializer_list>
int main() {
    if (geteuid() != 0) return 2;
    unsigned bad = 0;
    for (const char* root : {"/tmp", ""}) {
        char base[256], priv[256], pub[256], link[256];
        snprintf(base, sizeof(base), "%s/goal19-realpath-%ld", root, static_cast<long>(getpid()));
        snprintf(priv, sizeof(priv), "%s/private", base);
        snprintf(pub, sizeof(pub), "%s/public", base);
        snprintf(link, sizeof(link), "%s/link", base);
        if (mkdir(base, 0755) || mkdir(priv, 0710) || chown(priv, 0, 2000) || chmod(priv, 0710)) return 2;
        int fd = open(pub, O_CREAT | O_EXCL | O_WRONLY, 0644);
        if (fd < 0) return 2;
        if (close(fd) || chmod(pub, 0000)) return 2;
        // REALPATH needs ancestor search, not read access to the final file.
        if (symlink("private/../public", link)) return 2;
        for (unsigned context = 0; context < 5; ++context)
            for (bool grouped : {false, true}) {
                pid_t pid = fork();
                if (pid == 0) {
                    if (context == 1 && chdir(base)) _exit(2);
                    // Absolute lookup must ignore an inaccessible current directory.
                    if (context == 2 && chdir(priv)) _exit(2);
                    if (context >= 3 && ker::abi::vfs::wki_rule_add_vfs("/", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL)) _exit(2);
                    if (context == 4 && ker::abi::vfs::wki_rule_add_vfs("/unused-realpath-host-rule", ker::abi::vfs::WKI_VFS_ROUTE_HOST))
                        _exit(2);
                    gid_t group = 2000;
                    if (setgroups(grouped ? 1 : 0, grouped ? &group : nullptr) || setgid(2001) || setuid(1000)) _exit(2);
                    unsigned fail = 0;
                    const char* tails[] = {"public",    "private/../public", "private/.",          "private/missing",    "link",
                                           "public/..", "public/",           "private/../public/", "private//../public", "link/.."};
                    for (unsigned i = 0; i < 10; ++i) {
                        char path[512], out[512];
                        memset(out, 'Z', sizeof(out));
                        snprintf(path, sizeof(path), "%s%s%s", context != 1 ? base : "", context == 1 ? "" : "/", tails[i]);
                        int r = ker::abi::vfs::realpath(path, out, sizeof(out));
                        int expected = i == 0               ? 0
                                       : (i == 5 || i == 6) ? -ENOTDIR
                                       : i == 3             ? (grouped ? -ENOENT : -EACCES)
                                       : !grouped           ? -EACCES
                                       : (i == 7 || i == 9) ? -ENOTDIR
                                                            : 0;
                        bool untouched = true;
                        if (expected < 0)
                            for (char c : out) untouched &= c == 'Z';
                        bool ok = r == expected && untouched;
                        if (expected == 0 && r == 0) {
                            char canonical[512];
                            snprintf(canonical, sizeof(canonical), "%s/%s", base, i == 2 ? "private" : "public");
                            ok &= strcmp(out, canonical) == 0;
                        }
                        printf("%s fs=%s context=%u group=%d path=%s result=%d expected=%d untouched=%d\n", ok ? "PASS" : "FAIL",
                               *root ? "tmpfs" : "xfs", context, grouped, tails[i], r, expected, untouched);
                        fail += !ok;
                    }
                    fflush(stdout);
                    _exit(fail ? 1 : 0);
                }
                int status = 0;
                if (pid < 0 || waitpid(pid, &status, 0) != pid || !WIFEXITED(status) || WEXITSTATUS(status)) bad++;
            }
        if (unlink(link) || unlink(pub) || rmdir(priv) || rmdir(base)) return 2;
    }
    printf(
        "%s: 200 native realpath checks across TMPFS/XFS, absolute/relative paths, restricted cwd, LOCAL/HOST rules and group controls\n",
        bad ? "FAIL" : "PASS");
    return bad ? 1 : 0;
}
