#include <grp.h>
#include <signal.h>
#include <sys/process.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {
std::array<char, 65536> alternate{};
volatile sig_atomic_t observed = 0;
void handler(int) { observed = 1; }
char* replacement = nullptr;
char** replacement_args = nullptr;
void exec_handler(int) {
    execv(replacement, replacement_args);
    _exit(8);
}

auto after(uid_t uid, const char* expected_host, bool from_handler) -> int {
    alarm(30);
    uid_t real = 0, effective = 0, saved = 0;
    std::array<char, 128> host{};
    bool const IDENTITY = getresuid(&real, &effective, &saved) == 0 && real == uid && effective == uid && saved == uid;
    bool const HOST = gethostname(host.data(), host.size()) == 0 && std::strcmp(host.data(), expected_host) == 0;
    struct sigaction ignored{}, caught{}, term{};
    bool const IGNORE = sigaction(SIGUSR1, nullptr, &ignored) == 0 && ignored.sa_handler == SIG_IGN && !(ignored.sa_flags & SA_ONSTACK);
    bool const RESET = sigaction(SIGUSR2, nullptr, &caught) == 0 && caught.sa_handler == SIG_DFL &&
                       sigaction(SIGTERM, nullptr, &term) == 0 && term.sa_handler == SIG_DFL;
    sigset_t mask{}, pending{};
    bool const BLOCKED = sigprocmask(SIG_SETMASK, nullptr, &mask) == 0 && sigismember(&mask, SIGUSR2) == 1 &&
                         sigismember(&mask, SIGTERM) == (from_handler ? 1 : 0);
    bool const QUEUED = sigpending(&pending) == 0 && sigismember(&pending, SIGUSR2) == 1;
    stack_t stack{};
    bool const STACK = sigaltstack(nullptr, &stack) == 0 && (stack.ss_flags & SS_DISABLE) && !stack.ss_sp && !stack.ss_size;
    // Install the new image's handler before unblocking. A missing pending
    // signal is reported; never manufacture a replacement to pass this test.
    caught = {};
    caught.sa_handler = handler;
    sigemptyset(&caught.sa_mask);
    sigset_t release{};
    sigemptyset(&release);
    sigaddset(&release, SIGUSR2);
    bool const INSTALLED = sigaction(SIGUSR2, &caught, nullptr) == 0 && sigprocmask(SIG_UNBLOCK, &release, nullptr) == 0;
    for (unsigned i = 0; INSTALLED && QUEUED && !observed && i < 1000; ++i) usleep(1000);
    bool const PASS = IDENTITY && HOST && IGNORE && RESET && BLOCKED && QUEUED && STACK && INSTALLED && observed == 1;
    printf(
        "STATE host=%s pid=%ld uid=%u/%u/%u identity=%d host_ok=%d ignored=%d caught_reset=%d blocked=%d pending=%d "
        "altstack_disabled=%d delivered=%d\n",
        host.data(), static_cast<long>(getpid()), real, effective, saved, IDENTITY, HOST, IGNORE, RESET, BLOCKED, QUEUED, STACK,
        static_cast<int>(observed));
    return PASS ? 0 : 1;
}

auto before(bool remote, uid_t uid, char* local_image, char* remote_image, char* peer, char* owner, bool from_handler) -> int {
    alarm(30);
    if (setgroups(0, nullptr) || setgid(uid) || setuid(uid)) return 3;
    struct sigaction ignored{}, caught{};
    ignored.sa_handler = SIG_IGN;
    ignored.sa_flags = SA_ONSTACK;
    caught.sa_handler = handler;
    caught.sa_flags = SA_ONSTACK | SA_RESTART;
    sigemptyset(&ignored.sa_mask);
    sigemptyset(&caught.sa_mask);
    sigset_t blocked{};
    sigemptyset(&blocked);
    sigaddset(&blocked, SIGUSR2);
    stack_t stack{.ss_sp = alternate.data(), .ss_flags = 0, .ss_size = alternate.size()};
    if (sigaction(SIGUSR1, &ignored, nullptr) || sigaction(SIGUSR2, &caught, nullptr) || sigaltstack(&stack, nullptr) ||
        sigprocmask(SIG_SETMASK, &blocked, nullptr) || kill(getpid(), SIGUSR2))
        return 4;
    sigset_t pending{};
    if (sigpending(&pending) || sigismember(&pending, SIGUSR2) != 1) return 5;
    auto const PLACEMENT = remote ? ker::process::setwkitarget(peer, std::strlen(peer), ker::process::WKI_TARGET_FLAG_STRICT)
                                  : ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL);
    if (PLACEMENT != 0) return 6;
    std::array<char, 24> uid_text{};
    snprintf(uid_text.data(), uid_text.size(), "%u", uid);
    char after_flag[] = "--after";
    char handler_mode[] = "1";
    char plain_mode[] = "0";
    char* image = remote ? remote_image : local_image;
    char* args[] = {image, after_flag, uid_text.data(), remote ? peer : owner, from_handler ? handler_mode : plain_mode, nullptr};
    if (from_handler) {
        replacement = image;
        replacement_args = args;
        struct sigaction action{};
        action.sa_handler = exec_handler;
        action.sa_flags = SA_ONSTACK;
        sigemptyset(&action.sa_mask);
        if (sigaction(SIGTERM, &action, nullptr) || raise(SIGTERM)) return 8;
        return 9;
    }
    execv(image, args);
    printf("EXEC_ERROR placement=%s uid=%u errno=%d\n", remote ? "remote" : "local", uid, errno);
    return 7;
}
}  // namespace

int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc == 5 && std::strcmp(argv[1], "--after") == 0)
        return after(static_cast<uid_t>(std::strtoul(argv[2], nullptr, 10)), argv[3], std::strcmp(argv[4], "1") == 0);
    bool const FROM_HANDLER = argc == 5 && std::strcmp(argv[4], "--handler") == 0;
    if ((argc != 4 && !FROM_HANDLER) || ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL)) return 2;
    alarm(150);
    unsigned passed = 0;
    for (bool remote : {false, true}) {
        for (uid_t uid : {0U, 100U}) {
            pid_t const CHILD = fork();
            if (CHILD == 0) return before(remote, uid, argv[0], argv[3], argv[1], argv[2], FROM_HANDLER);
            int status = -1;
            pid_t result;
            do {
                result = CHILD > 0 ? waitpid(CHILD, &status, 0) : -1;
            } while (result < 0 && errno == EINTR);
            bool const REAPED = CHILD > 0 && result == CHILD;
            bool const PASS = REAPED && WIFEXITED(status) && WEXITSTATUS(status) == 0;
            passed += PASS ? 1 : 0;
            printf("%s placement=%s uid=%u handler_exec=%d source_pid=%ld status=%d reaped=%d\n", PASS ? "PASS" : "FAIL",
                   remote ? "remote" : "local", uid, FROM_HANDLER, static_cast<long>(CHILD), status, REAPED);
        }
    }
    printf("remote exec signal state: %u/4 cases passed\n", passed);
    return passed == 4 ? 0 : 1;
}
