#include <fcntl.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>

struct Writer {
    int fd;
    bool passed = false;
};

static auto write_all(int fd, const void* data, size_t size) -> bool {
    auto* bytes = static_cast<const char*>(data);
    while (size != 0) {
        ssize_t n = write(fd, bytes, size);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        bytes += n;
        size -= static_cast<size_t>(n);
    }
    return true;
}

static auto writer(void* argument) -> void* {
    auto* state = static_cast<Writer*>(argument);
    std::array<char, 4096> bytes{};
    bytes.fill(state->fd == STDOUT_FILENO ? 'A' : 'B');
    state->passed = true;
    for (unsigned i = 0; i < 64; ++i) {
        if (!write_all(state->fd, bytes.data(), bytes.size())) {
            state->passed = false;
            break;
        }
    }
    return nullptr;
}

int main(int argc, char** argv) {
    if (argc == 3 && std::strcmp(argv[1], "--reaped") == 0) {
        for (const char* p = argv[2]; *p != '\0'; ++p) {
            if (*p < '0' || *p > '9') return 2;
        }
        if (argv[2][0] == '\0') return 2;
        std::array<char, 64> path{};
        int n = snprintf(path.data(), path.size(), "/proc/%s", argv[2]);
        if (n < 0 || static_cast<size_t>(n) >= path.size()) return 2;
        struct stat st{};
        if (lstat(path.data(), &st) == 0) return 1;
        return errno == ENOENT ? 0 : 2;
    }
    if (argc != 2) return 2;
    std::array<char, 512> gate{}, report{};
    int g = snprintf(gate.data(), gate.size(), "%s/gate", argv[1]);
    int r = snprintf(report.data(), report.size(), "%s/report", argv[1]);
    if (g < 0 || r < 0 || static_cast<size_t>(g) >= gate.size() || static_cast<size_t>(r) >= report.size()) return 2;
    alarm(40);
    int ready[2];
    if (pipe(ready) != 0) return 3;
    pid_t child = fork();
    if (child < 0) return 3;
    if (child == 0) {
        alarm(40);
        close(ready[0]);
        if (!write_all(ready[1], "r", 1)) _exit(4);
        close(ready[1]);
        bool released = false;
        for (unsigned i = 0; i < 3000; ++i) {
            int fd = open(gate.data(), O_RDONLY);
            if (fd >= 0) {
                char byte = 0;
                released = read(fd, &byte, 1) == 1 && byte == 'x';
                close(fd);
                if (released) break;
            }
            usleep(10000);
        }
        bool const WRITES = released && write_all(STDOUT_FILENO, "late stdout", 11) && write_all(STDERR_FILENO, "late stderr", 11);
        // Closing both inherited Files releases the final capture owners.
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
        int fd = open(report.data(), O_WRONLY | O_CREAT | O_EXCL, 0600);
        if (fd < 0) _exit(5);
        std::array<char, 128> result{};
        int n = snprintf(result.data(), result.size(), "%s pid=%ld late_bytes=22\n", WRITES ? "PASS" : "FAIL", static_cast<long>(getpid()));
        bool const SAVED = n > 0 && static_cast<size_t>(n) < result.size() && write_all(fd, result.data(), static_cast<size_t>(n));
        close(fd);
        _exit(WRITES && SAVED ? 0 : 6);
    }
    close(ready[1]);
    char byte = 0;
    ssize_t started = 0;
    do {
        started = read(ready[0], &byte, 1);
    } while (started < 0 && errno == EINTR);
    close(ready[0]);
    if (started != 1) return 4;
    std::array<Writer, 2> states{{{STDOUT_FILENO}, {STDERR_FILENO}}};
    std::array<pthread_t, 2> threads{};
    size_t created = 0;
    for (; created < threads.size(); ++created) {
        if (pthread_create(&threads[created], nullptr, writer, &states[created]) != 0) break;
    }
    bool passed = created == threads.size();
    for (size_t i = 0; i < created; ++i) {
        passed = pthread_join(threads[i], nullptr) == 0 && states[i].passed && passed;
    }
    // The collector waits for this root's completion before creating the gate.
    return passed ? 0 : 7;
}
