// Ordinary remote OPEN lifetime regression: process-exit flush and bounded
// readonly cancellation reuse. Run locally against an owned remote workspace.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <string.h>
#include <sys/process.h>
#include <sys/wait.h>
#include <unistd.h>

auto main(int argc, char** argv) -> int {
    if (argc != 2 || geteuid() != 0) return 2;
    char path[512]{};
    if (snprintf(path, sizeof(path), "%s/exit-owned", argv[1]) >= static_cast<int>(sizeof(path))) return 2;
    for (int i = 0; i < 24; ++i) {
        char DATA[64]{};
        snprintf(DATA, sizeof(DATA), "ordinary-open-exit-flush-%d", i);
        pid_t child = fork();
        if (child < 0) return 2;
        if (child == 0) {
            using Control = ker::abi::process::CredentialControl;
            if (setgroups(0, nullptr) || ker::process::prctl(38, 1, 0, 0, 0) ||
                ker::process::prctl(static_cast<int>(Control::SET_SERVICE_PRIVILEGES), 3, 0, 0, 0) || setgid(2000) || setuid(1000) ||
                ker::process::prctl(static_cast<int>(Control::DROP_AUTHORITY_CEILING), 0, 0, 0, 0))
                _exit(2);
            int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0600);
            if (fd < 0 || write(fd, DATA, sizeof(DATA)) != sizeof(DATA)) _exit(3);
            // Deliberately leave the FD open. Waitability must follow final close.
            _exit(0);
        }
        int status = 0;
        if (waitpid(child, &status, 0) != child || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            printf("FAIL exit child=%d status=%d errno=%d\n", i, status, errno);
            return 1;
        }
        int fd = open(path, O_RDONLY);
        char received[sizeof(DATA) + 1]{};
        if (fd < 0 || read(fd, received, sizeof(received)) != sizeof(DATA) || memcmp(DATA, received, sizeof(DATA)) != 0 || close(fd)) {
            printf("FAIL exit visibility child=%d errno=%d\n", i, errno);
            return 1;
        }
    }
    // More completed opens than the ordinary outbox's entire capacity. A
    // bounded pause admits the deferred cancellation worker between iterations.
    unsigned backpressure = 0;
    for (int i = 0; i < 1100; ++i) {
        int fd = open(path, O_RDONLY);
        for (unsigned attempt = 0; fd < 0 && errno == EAGAIN && attempt < 200; ++attempt) {
            ++backpressure;
            usleep(10000);
            fd = open(path, O_RDONLY);
        }
        if (fd < 0 || close(fd)) {
            printf("FAIL readonly cancellation reuse iteration=%d errno=%d\n", i, errno);
            return 1;
        }
        usleep(1000);
    }
    if (unlink(path)) return 1;
    printf("PASS exit-close=24 readonly-cancellation-reuse=1100 backpressure=%u\n", backpressure);
    return 0;
}
