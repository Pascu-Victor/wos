// Disposable VM only: install root:root 06755 and use as dropbear-keygen's
// executable while retaining the production identity/profile and arguments.
// The probe checks the inherited restrictions, then execs real dropbearkey.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

extern char** environ;
static unsigned checks, failed;
static void expect(bool ok, const char* label) {
    ++checks;
    if (!ok) {
        ++failed;
        printf("keygen policy FAIL: %s errno=%d\n", label, errno);
    }
}
static int control(int option, unsigned long value) {
    // WOS PROCESS=3, PRCTL operation=36; the option values are the existing
    // credential-control ABI. WOS does not install Linux's sys/prctl.h.
    uint64_t result = 3;
    register uint64_t a4 __asm__("r8") = 0;
    register uint64_t a5 __asm__("r9") = 0;
    register uint64_t a6 __asm__("r10") = 0;
    __asm__ volatile("syscall"
                     : "+a"(result)
                     : "D"(36ULL), "S"((uint64_t)option), "d"((uint64_t)value), "r"(a4), "r"(a5), "r"(a6)
                     : "rcx", "r11", "memory");
    if ((int64_t)result < 0) {
        errno = (int)-(int64_t)result;
        return -1;
    }
    return (int)result;
}
static bool ids(void) {
    uid_t r, e, s;
    gid_t gr, ge, gs;
    return getresuid(&r, &e, &s) == 0 && getresgid(&gr, &ge, &gs) == 0 && r == 103 && e == 103 && s == 103 && gr == 103 && ge == 103 &&
           gs == 103;
}
int main(int argc, char** argv) {
    expect(ids(), "declared IDs survive set-ID exec");
    expect(getgroups(0, NULL) == 0, "no supplementary groups");
    expect(umask(0077) == 0077, "private umask");
    expect(environ && environ[0] == NULL, "empty environment");
    expect(control(39, 0) == 1, "NNP enabled");
    expect(control(0x574f5301, 0) == 0, "zero authority");
    expect(control(0x574f5304, 0) == 256, "empty sealed service grants");
    expect(setuid(0) == -1 && errno == EPERM, "cannot regain root UID");
    expect(seteuid(0) == -1 && errno == EPERM, "cannot regain effective root");
    expect(setgid(0) == -1 && errno == EPERM, "cannot regain root GID");
    gid_t root = 0;
    expect(setgroups(1, &root) == -1 && errno == EPERM, "cannot add root group");
    expect(control(0x574f5302, 65535) == -1 && errno == EPERM, "cannot restore authority");
    expect(control(0x574f5303, 1) == -1 && errno == EPERM, "cannot grant journal authority");
    expect(control(38, 0) == -1 && errno == EINVAL, "cannot clear NNP");
    expect(kill(1, 0) == -1 && errno == EPERM, "cannot signal init");
    expect(mount("none", "/tmp", "tmpfs", 0, NULL) == -1 && errno == EPERM, "cannot mount");
    int fd = socket(AF_INET, SOCK_RAW, 1);
    expect(fd == -1 && errno == EPERM, "cannot create raw socket");
    if (fd >= 0) close(fd);
    fd = open("/etc/goal19-keygen-denied", O_CREAT | O_EXCL | O_WRONLY, 0600);
    expect(fd == -1 && errno == EACCES, "cannot write outside key state");
    if (fd >= 0) {
        close(fd);
        unlink("/etc/goal19-keygen-denied");
    }
    fd = open("/var/log/journal/wos.journal", O_RDONLY);
    expect(fd == -1 && errno == EACCES, "cannot read journal state");
    if (fd >= 0) close(fd);
    struct stat state;
    expect(stat("/etc/dropbear", &state) == 0 && state.st_uid == 103 && state.st_gid == 103 && state.st_mode == (S_IFDIR | 0700),
           "private owned key directory");
    expect(ids() && getgroups(0, NULL) == 0 && control(39, 0) == 1 && control(0x574f5301, 0) == 0, "denials preserve credentials");
    printf("keygen policy: %u checks, %u failed\n", checks, failed);
    fflush(stdout);
    if (failed || argc < 2) return 1;
    argv[0] = "/bin/dropbearkey";
    execv(argv[0], argv);
    return 1;
}
