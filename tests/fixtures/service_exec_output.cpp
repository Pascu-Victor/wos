#include <termios.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

static auto read_byte(int fd) -> ssize_t {
    char byte = 0;
    ssize_t result = 0;
    do {
        result = read(fd, &byte, 1);
    } while (result < 0 && errno == EINTR);
    return result;
}

int main(int argc, char** argv) {
    if (argc != 3 && argc != 4) return 2;
    bool const inherited = argc == 4 && strcmp(argv[3], "inherited") == 0;
    bool const close_early = argc == 4 && strcmp(argv[3], "close") == 0;
    bool const terminal_single_stream = argc == 4 && strcmp(argv[3], "raw-terminal-single") == 0;
    bool const raw_terminal = terminal_single_stream || (argc == 4 && strcmp(argv[3], "raw-terminal") == 0);
    if (argc == 4 && !inherited && !close_early && !raw_terminal) return 2;
    char* end = nullptr;
    unsigned long remaining = strtoul(argv[1], &end, 10);
    if (!end || *end || !remaining || remaining > 1024 * 1024) return 2;
    const unsigned long requested = remaining;
    unsigned long hold_ms = strtoul(argv[2], &end, 10);
    if (!end || *end || hold_ms > 5000) return 2;
    if (raw_terminal) {
        termios attributes{};
        if (!isatty(STDOUT_FILENO) || tcgetattr(STDOUT_FILENO, &attributes)) return 6;
        cfmakeraw(&attributes);
        // This is a disposable SSH PTY. Keep it raw until channel retirement;
        // restoring early could translate output still queued on the home node.
        if (tcsetattr(STDOUT_FILENO, TCSANOW, &attributes)) return 6;
    }
    if (inherited) {
        int ready[2];
        if (pipe(ready) != 0) return 4;
        pid_t child = fork();
        if (child < 0) return 4;
        if (child == 0) {
            alarm(45);
            close(ready[0]);
            if (write(ready[1], "r", 1) != 1) _exit(4);
            close(ready[1]);
            // Keep both output writers until the collector observes root exit
            // and releases stdin. An all-writer EOF barrier cannot finish.
            _exit(read_byte(STDIN_FILENO) == 0 ? 0 : 4);
        }
        close(ready[1]);
        bool const started = read_byte(ready[0]) == 1;
        close(ready[0]);
        if (!started) return 4;
    }
    std::array<unsigned char, 4096> block{};
    for (size_t i = 0; i < block.size(); ++i) block[i] = static_cast<unsigned char>(i * 73 + 19);
    unsigned long emitted = 0;
    while (remaining) {
        size_t count = remaining < block.size() ? remaining : block.size();
        size_t offset = 0;
        while (offset < count) {
            ssize_t n = write(STDOUT_FILENO, block.data() + offset, count - offset);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) {
                fprintf(stderr, "WRITE_FAILED emitted=%lu errno=%d\n", emitted, errno);
                return 3;
            }
            offset += static_cast<size_t>(n);
            emitted += static_cast<unsigned long>(n);
        }
        remaining -= count;
    }
    if (terminal_single_stream) {
        if (dprintf(STDOUT_FILENO, "EMITTED bytes=%lu\n", requested) < 0) return 3;
    } else {
        fprintf(stderr, "EMITTED bytes=%lu\n", requested);
    }
    // Diagnostic control only: compare immediate exit with time for queued
    // output to drain. A passing delayed run is not a production fix.
    if (hold_ms) usleep(static_cast<useconds_t>(hold_ms * 1000));
    if (close_early) {
        alarm(45);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
        // The collector sends this byte only after observing output EOF while
        // this process is still alive. Completion must not retain File owners.
        if (read_byte(STDIN_FILENO) != 1) return 5;
    }
    return 0;
}
