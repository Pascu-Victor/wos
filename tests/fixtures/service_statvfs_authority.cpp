// Ordinary libc/syscall statvfs authorization checks in private guest directories.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/wait.h>
#include <unistd.h>

#include <initializer_list>

int main(int argc, char** argv) {
    if (argc != 2 || geteuid() != 0) return 2;
    const char* base = argv[1];
    char private_dir[512], private_file[512], public_file[512], cwd[512], moved[512], seed[512], moved_seed[512], link[512];
    snprintf(private_dir, sizeof(private_dir), "%s/private", base);
    snprintf(private_file, sizeof(private_file), "%s/private/seed", base);
    snprintf(public_file, sizeof(public_file), "%s/public", base);
    snprintf(cwd, sizeof(cwd), "%s/cwd", base);
    snprintf(moved, sizeof(moved), "%s/moved", base);
    snprintf(seed, sizeof(seed), "%s/cwd/seed", base);
    snprintf(moved_seed, sizeof(moved_seed), "%s/moved/seed", base);
    snprintf(link, sizeof(link), "%s/link", base);
    struct Cleanup {
        const char *base, *private_dir, *private_file, *public_file, *cwd, *moved, *seed, *moved_seed, *link;
        ~Cleanup() {
            chmod(base, 0755);
            for (auto* path : {link, private_file, public_file, seed, moved_seed}) unlink(path);
            for (auto* path : {private_dir, cwd, moved, base}) rmdir(path);
        }
    } cleanup{base, private_dir, private_file, public_file, cwd, moved, seed, moved_seed, link};
    if (mkdir(base, 0755) || mkdir(private_dir, 0700) || mkdir(cwd, 0755)) return 1;
    for (auto* path : {private_file, public_file, seed}) {
        int fd = open(path, O_CREAT | O_EXCL | O_RDWR, 0000);
        if (fd < 0 || close(fd)) return 1;
    }
    if (symlink(private_file, link)) return 1;
    struct statvfs expected{};
    if (statvfs(base, &expected) || statvfs(private_file, &expected) || statvfs(link, &expected)) return 1;
    for (unsigned profile = 0; profile != 2; ++profile) {
        pid_t child = fork();
        if (child < 0) return 1;
        if (child == 0) {
            unsigned checks = 0, failures = 0;
            auto query = [&](const char* path, int expected_error) {
                struct statvfs actual{};
                memset(&actual, 0x5A, sizeof(actual));
                auto const before = actual;
                errno = 0;
                int result = statvfs(path, &actual);
                int error = errno;
                bool okay = expected_error ? result == -1 && error == expected_error && !memcmp(&actual, &before, sizeof(actual))
                                           : result == 0 && actual.f_fsid == expected.f_fsid && actual.f_bsize == expected.f_bsize;
                ++checks;
                failures += !okay;
                printf("%s profile=%u case=%u errno=%d expected=%d\n", okay ? "PASS" : "FAIL", profile, checks, error, expected_error);
                fflush(stdout);
            };
            // Enter and rename the exact cwd while still root; replacement is empty.
            if (chdir(cwd) || rename(cwd, moved) || mkdir(cwd, 0700)) _exit(2);
            if (profile && (setgroups(0, nullptr) || setgid(100) || setuid(100))) _exit(3);
            query(public_file, 0);  // Metadata needs parent search, not file read access.
            query(private_file, profile ? EACCES : 0);
            query(link, profile ? EACCES : 0);
            char path[512];
            snprintf(path, sizeof(path), "%s/private/../public", base);
            query(path, profile ? EACCES : 0);
            snprintf(path, sizeof(path), "%s/absent", base);
            query(path, ENOENT);
            snprintf(path, sizeof(path), "%s/public/", base);
            query(path, ENOTDIR);
            snprintf(path, sizeof(path), "%s/public/..", base);
            query(path, ENOTDIR);
            query("", ENOENT);
            query("seed", 0);
            query("no-such-relative", ENOENT);
            query("seed/", ENOTDIR);
            printf("SUMMARY profile=%u checks=%u failures=%u\n", profile, checks, failures);
            fflush(stdout);
            _exit(failures ? 1 : 0);
        }
        int status = 0;
        if (waitpid(child, &status, 0) != child || !WIFEXITED(status) || WEXITSTATUS(status) != 0) return 1;
        if (rmdir(cwd) || rename(moved, cwd)) return 1;
    }
    if (unlink(link) || unlink(private_file) || unlink(public_file) || unlink(seed) || rmdir(private_dir) || rmdir(cwd) || rmdir(base))
        return 1;
    puts("PASS statvfs checks=22 cleanup=1");
    return 0;
}
