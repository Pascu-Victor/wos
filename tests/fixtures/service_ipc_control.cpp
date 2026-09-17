// Functional checks of already-open socket and PTY grants across WKI exec.
#include <arpa/inet.h>
#include <errno.h>
#include <grp.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/futex.h>
#include <sys/ioctl.h>
#include <sys/process.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <termios.h>
#include <unistd.h>

namespace {
unsigned checks{}, failures{};
void expect(bool ok, const char* name) {
    ++checks;
    if (!ok) {
        ++failures;
        printf("FAIL %s errno=%d\n", name, errno);
    }
}
bool drop(unsigned uid) { return setgroups(0, nullptr) == 0 && setgid(uid) == 0 && setuid(uid) == 0; }
void receiver(const char* expected, unsigned uid) {
    char host[256]{};
    expect(gethostname(host, sizeof(host)) == 0 && !strcmp(host, expected), "receiver hostname");
    uid_t r{}, e{}, s{};
    expect(getresuid(&r, &e, &s) == 0 && r == uid && e == uid && s == uid, "receiver identity");
    printf("RECEIVER pid=%ld host=%s uid=%u\n", static_cast<long>(getpid()), host, uid);
}
int result() {
    printf("%s checks=%u failed=%u\n", failures ? "FAIL" : "PASS", checks, failures);
    return failures ? 1 : 0;
}
int socket_probe(int fd, const char* host, unsigned uid, unsigned port) {
    receiver(host, uid);
    sockaddr_in peer{};
    socklen_t size = sizeof(peer);
    expect(getpeername(fd, reinterpret_cast<sockaddr*>(&peer), &size) == 0 && peer.sin_family == AF_INET &&
               peer.sin_addr.s_addr == htonl(INADDR_LOOPBACK) && ntohs(peer.sin_port) == port,
           "retained socket peer");
    int capacity = 8192;
    expect(setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &capacity, sizeof(capacity)) == 0, "retained socket setsockopt");
    capacity = 0;
    size = sizeof(capacity);
    expect(getsockopt(fd, SOL_SOCKET, SO_RCVBUF, &capacity, &size) == 0 && size == sizeof(capacity) && capacity == 8192,
           "retained socket getsockopt");
    expect(write(fd, "X", 1) == 1, "retained socket write");
    pollfd event{fd, POLLIN, 0};
    expect(poll(&event, 1, 5000) == 1 && (event.revents & POLLIN), "retained socket poll");
    char reply{};
    expect(read(fd, &reply, 1) == 1 && reply == 'Y', "retained socket read");
    expect(shutdown(fd, SHUT_WR) == 0, "retained socket shutdown");
    expect(close(fd) == 0, "retained socket close");
    return result() ? 1 : 23;
}
int socket_parent(const char* target, const char* image, unsigned uid) {
    int listener = socket(AF_INET, SOCK_STREAM, 0);
    if (listener < 0) return 2;
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) || listen(listener, 1)) return 3;
    socklen_t size = sizeof(address);
    if (getsockname(listener, reinterpret_cast<sockaddr*>(&address), &size)) return 4;
    fflush(stdout);
    pid_t child = fork();
    if (child == 0) {
        close(listener);
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0 || connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) || !drop(uid)) _exit(5);
        char fd_text[24], uid_text[24], port_text[24];
        snprintf(fd_text, sizeof(fd_text), "%d", fd);
        snprintf(uid_text, sizeof(uid_text), "%u", uid);
        snprintf(port_text, sizeof(port_text), "%u", ntohs(address.sin_port));
        if (ker::process::setwkitarget(target, strlen(target), ker::process::WKI_TARGET_FLAG_STRICT)) _exit(6);
        execl(image, image, "--probe", fd_text, target, uid_text, port_text, nullptr);
        _exit(7);
    }
    expect(child > 0, "owned socket child");
    int accepted = child > 0 ? accept(listener, nullptr, nullptr) : -1;
    close(listener);
    expect(accepted >= 0, "owned local connection");
    if (accepted >= 0) {
        char token{};
        expect(read(accepted, &token, 1) == 1 && token == 'X', "home reads remote byte");
        expect(write(accepted, "Y", 1) == 1, "home replies to remote receiver");
        expect(read(accepted, &token, 1) == 0, "remote shutdown reaches home");
        close(accepted);
    }
    int status{};
    pid_t waited = -1;
    if (child > 0) do {
            waited = waitpid(child, &status, 0);
        } while (waited < 0 && errno == EINTR);
    expect(waited == child && WIFEXITED(status) && WEXITSTATUS(status) == 23, "owned socket child normally reaped");
    printf("SOURCE child=%ld reaped=%d status=%d\n", static_cast<long>(child), waited == child, status);
    return result();
}
int pty_probe(const char* host, unsigned uid) {
    if (!drop(uid)) return 5;
    receiver(host, uid);
    expect(isatty(STDIN_FILENO), "retained PTY identity");
    termios settings{};
    expect(tcgetattr(STDIN_FILENO, &settings) == 0, "retained PTY TCGETS");
    expect(tcsetattr(STDIN_FILENO, TCSANOW, &settings) == 0, "retained PTY TCSETS");
    winsize original{}, changed{}, observed{};
    expect(ioctl(STDIN_FILENO, TIOCGWINSZ, &original) == 0 && original.ws_col == 80 && original.ws_row == 24, "retained PTY initial size");
    changed = original;
    changed.ws_col = 83;
    expect(ioctl(STDIN_FILENO, TIOCSWINSZ, &changed) == 0, "retained PTY size update");
    expect(ioctl(STDIN_FILENO, TIOCGWINSZ, &observed) == 0 && observed.ws_col == 83 && observed.ws_row == 24, "retained PTY changed size");
    expect(ioctl(STDIN_FILENO, TIOCSWINSZ, &original) == 0, "retained PTY size restore");
    return result();
}
int local_probe(const char* host, unsigned uid, const char* peer, const char* image) {
    if (!drop(uid)) return 5;
    receiver(host, uid);
    int word = 7;
    expect(ker::futex::wait(&word, 8, nullptr) == -EAGAIN, "local futex compares owned word");
    expect(ker::futex::wake(&word, 0) == 0, "local futex zero wake");
    expect(ker::futex::wake_all(&word) == 0 && word == 7, "local futex empty wake preserves word");
    int ends[2];
    if (pipe(ends)) return 6;
    int epoll = epoll_create1(0);
    expect(epoll >= 0, "local epoll create");
    if (epoll >= 0) {
        epoll_event event{};
        event.events = EPOLLIN;
        event.data.u64 = 0x12345678;
        expect(epoll_ctl(epoll, EPOLL_CTL_ADD, ends[0], &event) == 0, "local epoll add");
        fflush(stdout);
        pid_t child = fork();
        if (child == 0) {
            if (ker::process::setwkitarget(peer, strlen(peer), ker::process::WKI_TARGET_FLAG_STRICT)) _exit(6);
            execl(image, image, "--unexpected-epoll-export", nullptr);
            _exit(errno == ENOTSUP ? 23 : 7);
        }
        int status{};
        pid_t waited = -1;
        if (child > 0) do {
                waited = waitpid(child, &status, 0);
            } while (waited < 0 && errno == EINTR);
        expect(child > 0 && waited == child && WIFEXITED(status) && WEXITSTATUS(status) == 23,
               "remote exec refuses unexportable epoll before image replacement");
        printf("SOURCE child=%ld reaped=%d status=%d\n", static_cast<long>(child), waited == child, status);
        expect(write(ends[1], "E", 1) == 1, "owned pipe write after export denial");
        epoll_event observed{};
        expect(epoll_wait(epoll, &observed, 1, 1000) == 1 && (observed.events & EPOLLIN) && observed.data.u64 == event.data.u64,
               "local epoll interest survives export denial");
        char token{};
        expect(read(ends[0], &token, 1) == 1 && token == 'E', "owned pipe data survives export denial");
        expect(epoll_ctl(epoll, EPOLL_CTL_DEL, ends[0], nullptr) == 0, "local epoll delete");
        expect(close(epoll) == 0, "local epoll close");
    }
    expect(close(ends[0]) == 0 && close(ends[1]) == 0, "owned pipe close");
    return result();
}
}  // namespace
int main(int argc, char** argv) {
    alarm(30);
    if (argc == 6 && !strcmp(argv[1], "--probe")) return socket_probe(atoi(argv[2]), argv[3], atoi(argv[4]), atoi(argv[5]));
    if (argc == 5 && !strcmp(argv[1], "--socket")) return socket_parent(argv[2], argv[3], atoi(argv[4]));
    if (argc == 4 && !strcmp(argv[1], "--pty")) return pty_probe(argv[2], atoi(argv[3]));
    if (argc == 6 && !strcmp(argv[1], "--local")) return local_probe(argv[2], atoi(argv[3]), argv[4], argv[5]);
    return 2;
}
