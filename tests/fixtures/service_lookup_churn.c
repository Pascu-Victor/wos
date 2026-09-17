/* Native lookup liveness regression. The writer owns only a private tmp dir;
 * readers access an unchanged public library and their retained working dir. */
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

int main(int argc, char** argv) {
    int recreate = argc == 2 && !strcmp(argv[1], "recreate");
    int write_only = argc == 2 && !strcmp(argv[1], "write");
    if (argc != 2 || (!recreate && !write_only && strcmp(argv[1], "existing"))) return 2;
    char directory[96], path[128];
    snprintf(directory, sizeof(directory), "%s/goal19-lookup-%ld", write_only ? "/root" : "/tmp", (long)getpid());
    snprintf(path, sizeof(path), "%s/owned", directory);
    if (mkdir(directory, 0700) != 0) return 2;
    int seed = open(path, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (seed < 0) {
        rmdir(directory);
        return 2;
    }
    close(seed);
    if (chdir("/tmp") != 0) {
        unlink(path);
        rmdir(directory);
        return 2;
    }
    pid_t writer = fork();
    if (writer < 0) {
        unlink(path);
        rmdir(directory);
        return 2;
    }
    if (writer == 0) {
        unsigned failed = 0;
        if (write_only) {
            int fd = open(path, O_RDWR);
            if (fd < 0) _exit(3);
            for (unsigned i = 0; i < 10000; i++) {
                if (pwrite(fd, &i, sizeof(i), 0) != sizeof(i)) failed++;
            }
            if (close(fd) != 0) failed++;
            _exit(failed ? 3 : 0);
        }
        for (unsigned i = 0; i < 10000; i++) {
            if (recreate && unlink(path) != 0) failed++;
            int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0600);
            if (fd < 0)
                failed++;
            else
                close(fd);
        }
        _exit(failed ? 3 : 0);
    }
    unsigned open_fail = 0, cwd_fail = 0, wrong_cwd = 0;
    for (unsigned i = 0; i < 10000; i++) {
        int fd = open("/lib/libdl.so", O_RDONLY);
        if (fd < 0) {
            if (open_fail < 8) printf("open failure iteration=%u errno=%d\n", i, errno);
            open_fail++;
        } else
            close(fd);
        char cwd[4096];
        if (!getcwd(cwd, sizeof(cwd))) {
            if (cwd_fail < 8) printf("cwd failure iteration=%u errno=%d\n", i, errno);
            cwd_fail++;
        } else if (strcmp(cwd, "/tmp"))
            wrong_cwd++;
    }
    int status = 0;
    pid_t waited;
    do {
        waited = waitpid(writer, &status, 0);
    } while (waited < 0 && errno == EINTR);
    int cleanup_failed = unlink(path) != 0;
    cleanup_failed |= rmdir(directory) != 0;
    printf("lookup churn mode=%s open_fail=%u cwd_fail=%u wrong_cwd=%u writer_status=%d cleanup_failed=%d\n", argv[1], open_fail, cwd_fail,
           wrong_cwd, status, cleanup_failed);
    return open_fail || cwd_fail || wrong_cwd || waited != writer || status || cleanup_failed;
}
