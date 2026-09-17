#include <arpa/inet.h>
#include <fcntl.h>
#include <grp.h>
#include <signal.h>
#include <sys/process.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

static auto number(const char* text, unsigned long limit) -> unsigned long {
    char* end = nullptr;
    unsigned long value = strtoul(text, &end, 10);
    return end == text || *end || value > limit ? limit + 1 : value;
}

static auto identity_preserved(unsigned long uid) -> bool {
    uid_t real = 0, effective = 0, saved = 0;
    if (getresuid(&real, &effective, &saved) || real != uid || effective != uid || saved != uid) return false;
    if (uid != 0) {
        errno = 0;
        if (setuid(0) != -1 || errno != EPERM || getresuid(&real, &effective, &saved) || real != uid || effective != uid || saved != uid)
            return false;
        gid_t group_real = 0, group_effective = 0, group_saved = 0;
        if (getgroups(0, nullptr) != 0 || getresgid(&group_real, &group_effective, &group_saved) || group_real != uid ||
            group_effective != uid || group_saved != uid)
            return false;
        errno = 0;
        if (setgid(0) != -1 || errno != EPERM || getresgid(&group_real, &group_effective, &group_saved) || group_real != uid ||
            group_effective != uid || group_saved != uid)
            return false;
    }
    return true;
}

static auto release_received(int fd) -> bool {
    char byte = 0;
    ssize_t result = 0;
    do {
        result = read(fd, &byte, 1);
    } while (result < 0 && errno == EINTR);
    return result == 1 && byte == 'k';
}

static auto emit(int argc, char** argv) -> int {
    if (argc != 11) return 2;
    int fd = static_cast<int>(number(argv[2], 255));
    size_t count = number(argv[3], 1024 * 1024);
    unsigned long uid = number(argv[6], 100);
    unsigned long hold = number(argv[8], 1000);
    char host[64]{};
    if (fd > 255 || !count || count > 1024 * 1024 || uid > 100 || hold > 1000 || getuid() != uid || gethostname(host, sizeof(host)) ||
        strcmp(host, argv[7]))
        return 2;
    if (!identity_preserved(uid)) return 10;
    bool use_send = !strcmp(argv[4], "send");
    bool inherited = !strcmp(argv[5], "inherited");
    bool close_wait = !strcmp(argv[5], "close-wait");
    int release = static_cast<int>(number(argv[10], 255));
    if ((inherited || close_wait) && release > 255) return 2;
    bool invalid_shutdown = !strcmp(argv[5], "shutdown-invalid");
    bool read_shutdown = !strcmp(argv[5], "shutdown-read");
    bool half_close = !strcmp(argv[5], "shutdown") || invalid_shutdown;
    bool alias_shutdown = !strcmp(argv[5], "shutdown-alias");
    int alias = static_cast<int>(number(argv[9], 255));
    if (alias_shutdown && (alias > 255 || alias == fd)) return 2;
    if ((!use_send && strcmp(argv[4], "write")) ||
        (!half_close && !alias_shutdown && !read_shutdown && !inherited && !close_wait && strcmp(argv[5], "close")))
        return 2;
    if (invalid_shutdown) {
        for (int how : {-1, 3}) {
            errno = 0;
            if (shutdown(fd, how) != -1 || errno != EINVAL) return 11;
        }
    }
    if (read_shutdown && shutdown(fd, SHUT_RD)) return 12;
    alarm(45);
    std::array<unsigned char, 4096> block{};
    for (size_t i = 0; i < block.size(); ++i) block[i] = static_cast<unsigned char>(i * 73 + 19);
    size_t accepted = 0;
    while (accepted < count) {
        size_t length = count - accepted < block.size() ? count - accepted : block.size();
        size_t offset = 0;
        while (offset < length) {
            ssize_t n = use_send ? send(fd, block.data() + offset, length - offset, 0) : write(fd, block.data() + offset, length - offset);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) {
                fprintf(stderr, "SOCKET_WRITE_FAILED host=%s uid=%u accepted=%zu errno=%d\n", host, getuid(), accepted + offset, errno);
                return 3;
            }
            offset += static_cast<size_t>(n);
        }
        accepted += length;
    }
    fprintf(stderr, "SOCKET_ACCEPTED host=%s uid=%u bytes=%zu\n", host, getuid(), accepted);
    // Diagnostic only; default runs have no hold before close/shutdown.
    if (hold) usleep(static_cast<useconds_t>(hold * 1000));
    if (inherited) {
        pid_t descendant = fork();
        if (descendant < 0) return 13;
        if (descendant == 0) {
            alarm(45);
            close(STDIN_FILENO);
            close(STDOUT_FILENO);
            close(STDERR_FILENO);
            if (!release_received(release) || !identity_preserved(uid)) _exit(13);
            // This write occurs only after the source has reaped the root.
            // The inherited descriptor must retain its authority and lifetime.
            if (write(fd, "k", 1) != 1) _exit(13);
            _exit(close(fd) ? 13 : 0);
        }
        return 0;
    }
    if (close_wait) {
        if (close(fd) || !release_received(release)) return 14;
        return 0;
    }
    if (half_close || alias_shutdown || read_shutdown) {
        if (shutdown(alias_shutdown ? alias : fd, SHUT_WR)) {
            fprintf(stderr, "SOCKET_SHUTDOWN_FAILED errno=%d\n", errno);
            return 4;
        }
        // Alias case qualifies delivery/EOF only; the original shutdown case
        // continues to require a reply on its surviving read side.
        if (alias_shutdown || read_shutdown) {
            int rc = alias_shutdown ? close(alias) : 0;
            return close(fd) || rc ? 6 : 0;
        }
        char reply = 0;
        ssize_t n = 0;
        do {
            n = read(fd, &reply, 1);
        } while (n < 0 && errno == EINTR);
        if (n != 1 || reply != 'k') {
            fprintf(stderr, "SOCKET_REPLY_FAILED count=%ld errno=%d byte=%d\n", static_cast<long>(n), errno, reply);
            return 5;
        }
    }
    return close(fd) ? 6 : 0;
}

int main(int argc, char** argv) {
    signal(SIGPIPE, SIG_IGN);
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc >= 2 && !strcmp(argv[1], "--emit")) return emit(argc, argv);
    if (argc != 7 || getuid() != 0 || argv[0][0] != '/') return 2;
    unsigned long count = number(argv[2], 1024 * 1024);
    unsigned long uid = number(argv[5], 100);
    unsigned long hold = number(argv[6], 1000);
    bool local = !strcmp(argv[1], "local");
    bool inherited = !strcmp(argv[4], "inherited");
    bool close_wait = !strcmp(argv[4], "close-wait");
    bool half_close = !strcmp(argv[4], "shutdown") || !strcmp(argv[4], "shutdown-invalid");
    bool read_shutdown = !strcmp(argv[4], "shutdown-read");
    bool alias_shutdown = !strcmp(argv[4], "shutdown-alias");
    if (!count || count > 1024 * 1024 || (uid != 0 && uid != 100) || hold > 1000 || (strcmp(argv[3], "write") && strcmp(argv[3], "send")) ||
        (!half_close && !alias_shutdown && !read_shutdown && !inherited && !close_wait && strcmp(argv[4], "close")) ||
        ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL))
        return 2;
    alarm(60);
    char owner[64]{};
    if (gethostname(owner, sizeof(owner))) return 2;
    int listener = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t address_len = sizeof(address);
    if (listener < 0 || bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) || listen(listener, 1) ||
        getsockname(listener, reinterpret_cast<sockaddr*>(&address), &address_len))
        return 7;
    std::array<int, 2> release{-1, -1};
    if ((inherited || close_wait) && pipe(release.data())) return 7;
    pid_t child = fork();
    if (child < 0) return 7;
    if (child == 0) {
        if (release[1] >= 0) close(release[1]);
        alarm(45);
        close(listener);
        int endpoint = socket(AF_INET, SOCK_STREAM, 0);
        if (endpoint < 0 || connect(endpoint, reinterpret_cast<sockaddr*>(&address), sizeof(address)) || fcntl(endpoint, F_SETFD, 0))
            _exit(7);
        int alias = alias_shutdown ? fcntl(endpoint, F_DUPFD, 40) : 256;
        if (alias_shutdown && (alias < 0 || alias > 255 || fcntl(alias, F_SETFD, 0))) _exit(7);
        // The connected endpoint is inherited authority. Drop identity before
        // either local or remote exec, without reopening or rebinding it.
        if (uid && (setgroups(0, nullptr) || setgid(static_cast<gid_t>(uid)) || setuid(static_cast<uid_t>(uid)))) _exit(8);
        if (!local && ker::process::setwkitarget(argv[1], strlen(argv[1]), ker::process::WKI_TARGET_FLAG_STRICT)) _exit(9);
        char path[1024]{};
        int length = local ? snprintf(path, sizeof(path), "%s", argv[0]) : snprintf(path, sizeof(path), "/wki/%s%s", owner, argv[0]);
        if (length < 0 || static_cast<size_t>(length) >= sizeof(path)) _exit(9);
        char descriptor[16]{};
        snprintf(descriptor, sizeof(descriptor), "%d", endpoint);
        char alias_descriptor[16]{};
        snprintf(alias_descriptor, sizeof(alias_descriptor), "%d", alias);
        char release_descriptor[16]{};
        snprintf(release_descriptor, sizeof(release_descriptor), "%d", release[0] >= 0 ? release[0] : 256);
        char mode[] = "--emit";
        char* args[]{path,
                     mode,
                     descriptor,
                     argv[2],
                     argv[3],
                     argv[4],
                     argv[5],
                     local ? owner : argv[1],
                     argv[6],
                     alias_descriptor,
                     release_descriptor,
                     nullptr};
        execv(path, args);
        fprintf(stderr, "SOCKET_EXEC_FAILED errno=%d\n", errno);
        _exit(9);
    }
    if (release[0] >= 0) close(release[0]);
    int endpoint = accept(listener, nullptr, nullptr);
    close(listener);
    size_t received = 0;
    bool exact = true, eof = false;
    int read_error = 0;
    int status = 0;
    pid_t waited = 0;
    bool released = false, late = false;
    std::array<unsigned char, 8192> block{};
    if (endpoint >= 0) {
        for (;;) {
            if (inherited && received == count && !released) {
                do {
                    waited = waitpid(child, &status, 0);
                } while (waited < 0 && errno == EINTR);
                if (waited != child || !WIFEXITED(status) || WEXITSTATUS(status) != 0 || write(release[1], "k", 1) != 1) break;
                released = true;
                late = release_received(endpoint);
                if (!late) break;
            }
            size_t limit = inherited && received < count ? count - received : block.size();
            if (limit > block.size()) limit = block.size();
            ssize_t n = read(endpoint, block.data(), limit);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) {
                eof = n == 0;
                if (n < 0) read_error = errno;
                break;
            }
            for (ssize_t i = 0; i < n; ++i)
                exact = exact && block[static_cast<size_t>(i)] == static_cast<unsigned char>((received + i) * 73 + 19);
            received += static_cast<size_t>(n);
            if (received > count) {
                exact = false;
                break;
            }
        }
    } else
        read_error = errno;
    bool replied = !half_close;
    if (half_close && eof) replied = write(endpoint, "k", 1) == 1;
    if (endpoint >= 0) close(endpoint);
    if (close_wait && eof) released = write(release[1], "k", 1) == 1;
    if (release[1] >= 0) close(release[1]);
    if (waited == 0) {
        do {
            waited = waitpid(child, &status, 0);
        } while (waited < 0 && errno == EINTR);
    }
    bool passed = waited == child && WIFEXITED(status) && WEXITSTATUS(status) == 0 && received == count && exact && eof && replied;
    if (inherited || close_wait) {
        passed = passed && released && (!inherited || late);
        printf("SOCKET_LIFECYCLE end=%s released=%d late=%d\n", argv[4], released, late);
    }
    printf(
        "SOCKET_RESULT target=%s uid=%lu api=%s end=%s requested=%lu received=%zu exact=%d eof=%d replied=%d read_errno=%d status=%d "
        "passed=%d\n",
        argv[1], uid, argv[3], argv[4], count, received, exact, eof, replied, read_error, status, passed);
    return passed ? 0 : 1;
}
