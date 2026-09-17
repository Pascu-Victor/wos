/* Disposable-VM session relay fixture: bounded duplex data and inherited FDs. */
#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int write_all(int fd, const char* data, size_t size) {
    while (size) {
        ssize_t n = write(fd, data, size);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return 0;
        data += n;
        size -= n;
    }
    return 1;
}

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    if (!strcmp(argv[1], "fds")) {
        DIR* directory = opendir("/proc/self/fd");
        struct dirent* entry;
        int own, unexpected = 0;
        if (!directory) return 2;
        own = dirfd(directory);
        while ((entry = readdir(directory))) {
            char* end;
            long fd = strtol(entry->d_name, &end, 10);
            if (*entry->d_name && !*end && fd > 2 && fd != own) unexpected++;
        }
        closedir(directory);
        printf("unexpected_fds=%d uid=%ld gid=%ld\n", unexpected, (long)getuid(), (long)getgid());
        return unexpected != 0;
    }
    if (!strcmp(argv[1], "duplex")) {
        char data[4096];
        for (;;) {
            ssize_t n = read(STDIN_FILENO, data, sizeof(data));
            if (n < 0 && errno == EINTR) continue;
            if (n < 0) return 2;
            if (!n) return 37;
            if (!write_all(STDOUT_FILENO, data, n) || !write_all(STDERR_FILENO, data, n)) return 2;
        }
    }
    if (!strcmp(argv[1], "output")) {
        char data[4096];
        memset(data, 'O', sizeof(data));
        for (int i = 0; i < 512; i++)
            if (!write_all(STDOUT_FILENO, data, sizeof(data))) return 2;
        memset(data, 'E', sizeof(data));
        for (int i = 0; i < 512; i++)
            if (!write_all(STDERR_FILENO, data, sizeof(data))) return 2;
        return 29;
    }
    if (!strcmp(argv[1], "wait")) {
        if (!write_all(STDOUT_FILENO, "ready\n", 6)) return 2;
        /* WOS has no pause sysdep. Block on the session's stdin pipe. */
        char byte;
        for (;;) {
            ssize_t n = read(STDIN_FILENO, &byte, 1);
            if (n == 0) return 0;
            if (n < 0 && errno != EINTR) return 2;
        }
    }
    return 2;
}
