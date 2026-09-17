/* Disposable-VM PTY fixture. The host controls each bounded exchange. */
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <termios.h>
#include <unistd.h>

static int write_all(int fd, const void* data, size_t size) {
    const char* cursor = data;
    while (size) {
        ssize_t n = write(fd, cursor, size);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return 0;
        cursor += n;
        size -= n;
    }
    return 1;
}

static int inspect(void) {
    struct stat st;
    struct termios modes;
    struct winsize window;
    const char* term = getenv("TERM");
    const char* tty = getenv("SSH_TTY");
    DIR* directory;
    struct dirent* entry;
    int own, unexpected = 0;
    if (!term || !tty || tcgetattr(0, &modes) || ioctl(0, TIOCGWINSZ, &window)) return 2;
    for (int fd = 0; fd < 3; fd++) {
        if (!isatty(fd) || fstat(fd, &st) || st.st_uid != geteuid() || st.st_gid != getegid() || (st.st_mode & 0777) != 0600) return 3;
    }
    if (stat(tty, &st) || st.st_uid != geteuid() || st.st_gid != getegid() || (st.st_mode & 0777) != 0600) return 4;
    directory = opendir("/proc/self/fd");
    if (!directory) return 5;
    own = dirfd(directory);
    while ((entry = readdir(directory))) {
        char* end;
        long fd = strtol(entry->d_name, &end, 10);
        if (*entry->d_name && !*end && fd > 2 && fd != own) unexpected++;
    }
    closedir(directory);
    printf(
        "pid=%ld parent=%ld sid=%ld pgrp=%ld foreground=%ld uid=%ld gid=%ld mode=600 fds=%d "
        "term=%s tty=%s cols=%u rows=%u x=%u y=%u echo=%d canonical=%d opost=%d\n",
        (long)getpid(), (long)getppid(), (long)getsid(0), (long)getpgrp(), (long)tcgetpgrp(0), (long)geteuid(), (long)getegid(), unexpected,
        term, tty, window.ws_col, window.ws_row, window.ws_xpixel, window.ws_ypixel, !!(modes.c_lflag & ECHO), !!(modes.c_lflag & ICANON),
        !!(modes.c_oflag & OPOST));
    fflush(stdout);
    return unexpected ? 6 : 0;
}

static void hangup(int signal_number) {
    (void)signal_number;
    _exit(41);
}

int main(int argc, char** argv) {
    char data[4096];
    if (argc != 2) return 2;
    if (!strcmp(argv[1], "inspect")) return inspect();
    if (!strcmp(argv[1], "resize")) {
        struct winsize window;
        int status = inspect();
        if (status) return status;
        /* Wake by input after the host sends window-change, not by SIGWINCH. */
        for (int i = 0; i < 2; i++) {
            if (read(0, data, 1) != 1 || ioctl(0, TIOCGWINSZ, &window)) return 7;
            printf("resized=%u,%u,%u,%u\n", window.ws_col, window.ws_row, window.ws_xpixel, window.ws_ypixel);
            fflush(stdout);
        }
        return 17;
    }
    if (!strcmp(argv[1], "duplex")) {
        size_t remaining = 256 * 1024;
        while (remaining) {
            ssize_t n = read(0, data, remaining < sizeof(data) ? remaining : sizeof(data));
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0 || !write_all(1, data, n)) return 8;
            remaining -= n;
        }
        if (!write_all(2, "merged-stderr", 13)) return 9;
        return 37;
    }
    if (!strcmp(argv[1], "output")) {
        memset(data, 'P', sizeof(data));
        for (int i = 0; i < 512; i++)
            if (!write_all(i % 2 ? 2 : 1, data, sizeof(data))) return 10;
        return 29;
    }
    if (!strcmp(argv[1], "wait") || !strcmp(argv[1], "wait-hup") || !strcmp(argv[1], "wait-ignore-hup")) {
        /* Install before the ready snapshot so host EOF cannot race setup. */
        if (!strcmp(argv[1], "wait-hup") && signal(SIGHUP, hangup) == SIG_ERR) return 12;
        if (!strcmp(argv[1], "wait-ignore-hup") && signal(SIGHUP, SIG_IGN) == SIG_ERR) return 12;
        int status = inspect();
        if (status) return status;
        for (;;) {
            ssize_t n = read(0, data, sizeof(data));
            if (n < 0 && errno == EINTR) continue;
            if (n < 0) return 11;
            if (!n) return 0;
        }
    }
    return 2;
}
