#include <fcntl.h>
#include <grp.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <initializer_list>
int check(bool real_root, const char* base) {
    char file[512], leaf[512];
    snprintf(file, sizeof(file), "%s/file", base);
    snprintf(leaf, sizeof(leaf), "%s/hidden/leaf", base);
    uid_t real = real_root ? 0 : 1002, effective = real_root ? 1002 : 0;
    uid_t a, b, c;
    if (getresuid(&a, &b, &c) || a != real || b != effective) {
        printf("SETUP_IDS actual=%u/%u/%u expected=%u/%u\n", a, b, c, real, effective);
        return 7;
    }
    unsigned bad = 0;
    for (const char* path : {file, leaf})
        for (bool effective_check : {false, true}) {
            errno = 0;
            int rc = effective_check ? faccessat(AT_FDCWD, path, R_OK, AT_EACCESS) : access(path, R_OK);
            int error = rc < 0 ? errno : 0;
            bool allowed = effective_check ? !real_root : real_root;
            bool okay = allowed ? rc == 0 : rc == -1 && error == EACCES;
            printf("%s real=%u effective=%u path=%s check=%s rc=%d errno=%d expected=%s\n", okay ? "PASS" : "FAIL", real, effective,
                   path == file ? "file" : "hidden/leaf", effective_check ? "effective" : "real", rc, error,
                   allowed ? "allowed" : "EACCES");
            bad += !okay;
        }
    errno = 0;
    int fd = open(file, O_RDONLY);
    int error = fd < 0 ? errno : 0;
    bool okay = real_root ? fd < 0 && error == EACCES : fd >= 0;
    if (fd >= 0) close(fd);
    printf("%s effective-open real=%u effective=%u errno=%d\n", okay ? "PASS" : "FAIL", real, effective, error);
    bad += !okay;
    return bad ? 1 : 0;
}
int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc == 3 && !strcmp(argv[1], "--profile")) return check(false, argv[2]);
    if (argc != 2 || geteuid() != 0) return 2;
    const char* base = argv[1];
    char file[512], hidden[512], leaf[512];
    snprintf(file, sizeof(file), "%s/file", base);
    snprintf(hidden, sizeof(hidden), "%s/hidden", base);
    snprintf(leaf, sizeof(leaf), "%s/hidden/leaf", base);
    if (mkdir(base, 0755) || chmod(base, 0755) || mkdir(hidden, 0700) || chmod(hidden, 0700)) return 3;
    for (const char* p : {file, leaf}) {
        int fd = open(p, O_CREAT | O_EXCL | O_WRONLY, 0600);
        if (fd < 0 || close(fd) || chmod(p, p == file ? 0600 : 0644)) return 4;
    }
    unsigned failures = 0;
    for (bool real_root : {false, true}) {
        pid_t pid = fork();
        if (pid < 0) return 5;
        if (pid == 0) {
            if (setgroups(0, nullptr)) _exit(6);
            if (real_root) {
                if (setegid(1002) || seteuid(1002)) _exit(6);
                _exit(check(true, base));
            }
            if (setgid(1002) || setuid(1002)) _exit(6);
            execl(argv[0], argv[0], "--profile", base, nullptr);
            printf("SEED_EXEC errno=%d\n", errno);
            _exit(6);
        }
        int status = 0;
        if (waitpid(pid, &status, 0) != pid || !WIFEXITED(status)) return 8;
        printf("PROFILE_EXIT real_root=%d status=%d\n", real_root, WEXITSTATUS(status));
        failures += WEXITSTATUS(status) != 0;
    }
    if (unlink(leaf) || rmdir(hidden) || unlink(file) || rmdir(base)) return 9;
    printf("ACCESS_IDENTITY profiles_failed=%u cleanup=1\n", failures);
    return failures ? 1 : 0;
}
