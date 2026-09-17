#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/process.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <atomic>

static auto identity(unsigned uid) -> bool {
    uid_t real = 0, effective = 0, saved = 0;
    if (getresuid(&real, &effective, &saved) || real != uid || effective != uid || saved != uid) return false;
    if (uid == 0) return true;
    gid_t gr = 0, ge = 0, gs = 0;
    if (getgroups(0, nullptr) != 0 || getresgid(&gr, &ge, &gs) || gr != uid || ge != uid || gs != uid) return false;
    errno = 0;
    if (setuid(0) != -1 || errno != EPERM || getresuid(&real, &effective, &saved) || real != uid || effective != uid || saved != uid)
        return false;
    errno = 0;
    return setgid(0) == -1 && errno == EPERM && getresgid(&gr, &ge, &gs) == 0 && gr == uid && ge == uid && gs == uid;
}

struct Reader {
    int fd = -1;
    unsigned uid = 0;
    bool use_recv = false;
    bool nonblock = false;
    bool concurrent = false;
    size_t count = 0;
    std::atomic<unsigned> started{0}, ended{0}, errors{0};
    std::atomic<size_t> received{0};
    std::array<std::atomic<unsigned>, 256> seen{};
};

static auto receive(Reader& state, void* data, size_t size) -> ssize_t {
    return state.use_recv ? recv(state.fd, data, size, 0) : read(state.fd, data, size);
}

static auto reader(void* opaque) -> void* {
    auto& state = *static_cast<Reader*>(opaque);
    if (!identity(state.uid)) state.errors.fetch_add(1);
    state.started.fetch_add(1);
    std::array<unsigned char, 8192> buffer{};
    size_t position = 0;
    for (;;) {
        ssize_t n = receive(state, buffer.data(), state.concurrent ? 1 : buffer.size());
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && errno == EAGAIN && state.nonblock) {
            pollfd pfd{state.fd, POLLIN, 0};
            if (poll(&pfd, 1, 5000) <= 0) {
                state.errors.fetch_add(1);
                break;
            }
            continue;
        }
        if (n <= 0) {
            if (n == 0)
                state.ended.fetch_add(1);
            else
                state.errors.fetch_add(1);
            break;
        }
        state.received.fetch_add(static_cast<size_t>(n));
        if (state.concurrent) {
            // Every input byte is unique in this mode. Duplicate consumption
            // or omission is observable independent of reader scheduling.
            state.seen[buffer[0]].fetch_add(1);
        } else {
            for (ssize_t i = 0; i < n; ++i, ++position)
                if (buffer[static_cast<size_t>(i)] != static_cast<unsigned char>(position * 73 + 19)) state.errors.fetch_add(1);
        }
        if (state.received.load() > state.count) {
            state.errors.fetch_add(1);
            break;
        }
    }
    return nullptr;
}

static auto consume(int argc, char** argv) -> int {
    if (argc != 10) return 2;
    Reader state;
    state.fd = atoi(argv[2]);
    state.count = strtoul(argv[3], nullptr, 10);
    state.use_recv = !strcmp(argv[4], "recv");
    unsigned readers = static_cast<unsigned>(atoi(argv[5]));
    state.concurrent = readers > 1;
    state.uid = static_cast<unsigned>(atoi(argv[6]));
    state.nonblock = !strcmp(argv[8], "nonblock");
    char host[64]{};
    if (!identity(state.uid) || gethostname(host, sizeof(host)) || strcmp(host, argv[7]) || readers < 1 || readers > 40 ||
        (state.concurrent && state.count > 256))
        return 3;
    alarm(45);
    char byte = 0;
    // Before the source sees 'k', it has sent no bytes and kept its writer open.
    if (receive(state, &byte, 0) != 0) return 4;
    if (state.nonblock) {
        errno = 0;
        if (receive(state, &byte, 1) != -1 || errno != EAGAIN) return 5;
    }
    std::array<pthread_t, 40> threads{};
    if (state.concurrent) {
        for (unsigned i = 0; i < readers; ++i)
            if (pthread_create(&threads[i], nullptr, reader, &state)) return 6;
        while (state.started.load() != readers) usleep(1000);
    }
    if (write(state.fd, "k", 1) != 1) return 7;
    if (state.concurrent) {
        for (unsigned i = 0; i < readers; ++i)
            if (pthread_join(threads[i], nullptr)) return 8;
    } else
        reader(&state);
    bool exact = state.received.load() == state.count && state.errors.load() == 0 && state.ended.load() == readers;
    if (state.concurrent) {
        for (size_t i = 0; i < state.seen.size(); ++i) exact = exact && state.seen[i].load() == (i < state.count ? 1U : 0U);
    }
    if (!identity(state.uid)) exact = false;
    fprintf(stderr, "SOCKET_INPUT host=%s uid=%u bytes=%zu readers=%u eof=%u exact=%d\n", host, state.uid, state.received.load(), readers,
            state.ended.load(), exact);
    if (!exact || write(state.fd, "k", 1) != 1) return 9;
    return close(state.fd) ? 10 : 0;
}

int main(int argc, char** argv) {
    signal(SIGPIPE, SIG_IGN);
    if (argc > 1 && !strcmp(argv[1], "--consume")) return consume(argc, argv);
    if (argc != 7 || getuid() != 0 || argv[0][0] != '/') return 2;
    size_t count = strtoul(argv[2], nullptr, 10);
    unsigned readers = static_cast<unsigned>(atoi(argv[4]));
    unsigned uid = static_cast<unsigned>(atoi(argv[5]));
    bool nonblock = !strcmp(argv[6], "nonblock"), local = !strcmp(argv[1], "local");
    if (!count || count > 1048576 || readers < 1 || readers > 40 || (readers > 1 && count > 256) || (uid != 0 && uid != 100) ||
        (strcmp(argv[3], "read") && strcmp(argv[3], "recv")) || (!nonblock && strcmp(argv[6], "block")) ||
        ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL))
        return 2;
    alarm(60);
    char owner[64]{};
    if (gethostname(owner, sizeof(owner))) return 2;
    int listener = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t length = sizeof(address);
    if (listener < 0 || bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) || listen(listener, 1) ||
        getsockname(listener, reinterpret_cast<sockaddr*>(&address), &length))
        return 11;
    pid_t child = fork();
    if (child < 0) return 11;
    if (child == 0) {
        close(listener);
        int endpoint = socket(AF_INET, SOCK_STREAM, 0);
        if (endpoint < 0 || connect(endpoint, reinterpret_cast<sockaddr*>(&address), sizeof(address)) || fcntl(endpoint, F_SETFD, 0) ||
            (nonblock && fcntl(endpoint, F_SETFL, O_NONBLOCK)))
            _exit(11);
        if (uid && (setgroups(0, nullptr) || setgid(uid) || setuid(uid))) _exit(12);
        if (!local && ker::process::setwkitarget(argv[1], strlen(argv[1]), ker::process::WKI_TARGET_FLAG_STRICT)) _exit(13);
        char path[1024]{}, fd[16]{};
        snprintf(fd, sizeof(fd), "%d", endpoint);
        int n = local ? snprintf(path, sizeof(path), "%s", argv[0]) : snprintf(path, sizeof(path), "/wki/%s%s", owner, argv[0]);
        if (n < 0 || static_cast<size_t>(n) >= sizeof(path)) _exit(13);
        char mode[] = "--consume", reserved[] = "0";
        char* args[]{path, mode, fd, argv[2], argv[3], argv[4], argv[5], local ? owner : argv[1], argv[6], reserved, nullptr};
        execv(path, args);
        _exit(13);
    }
    int endpoint = accept(listener, nullptr, nullptr);
    close(listener);
    char byte = 0;
    bool ready = endpoint >= 0 && read(endpoint, &byte, 1) == 1 && byte == 'k';
    // Let all concurrent readers park before DATA and then EOF wake them.
    if (ready) usleep(150000);
    size_t sent = 0;
    std::array<unsigned char, 8192> buffer{};
    while (ready && sent < count) {
        size_t length = count - sent < buffer.size() ? count - sent : buffer.size();
        for (size_t i = 0; i < length; ++i) buffer[i] = static_cast<unsigned char>(readers > 1 ? sent + i : (sent + i) * 73 + 19);
        ssize_t n = write(endpoint, buffer.data(), length);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) break;
        sent += static_cast<size_t>(n);
    }
    bool eof = sent == count && shutdown(endpoint, SHUT_WR) == 0;
    bool reply = eof && read(endpoint, &byte, 1) == 1 && byte == 'k';
    if (endpoint >= 0) close(endpoint);
    int status = 0;
    pid_t waited;
    do {
        waited = waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    bool passed = ready && eof && reply && waited == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
    printf("SOCKET_INPUT_RESULT target=%s uid=%u api=%s mode=%s bytes=%zu readers=%u status=%d passed=%d\n", argv[1], uid, argv[3], argv[6],
           count, readers, status, passed);
    return passed ? 0 : 1;
}
