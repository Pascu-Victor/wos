// Each case owns a child; successful exec returns to this same uploaded helper.
#include <errno.h>
#include <grp.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/process.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
char alternate[65536];
char* image;
char* next_args[5];
volatile sig_atomic_t delivered;
unsigned failures;
unsigned failed_exec_release;
void* failed_exec_worker(void*) {
    while (!__atomic_load_n(&failed_exec_release, __ATOMIC_ACQUIRE)) usleep(1000);
    return &failed_exec_release;
}
void caught(int) { delivered = 1; }
void exec_handler(int) {
    execv(image, next_args);
    _exit(91);
}
void check(bool ok, const char* label) {
    if (!ok) {
        ++failures;
        printf("FAIL check=%s errno=%d\n", label, errno);
    }
}
bool drop(unsigned profile) {
    using Control = ker::abi::process::CredentialControl;
    if (profile == 0) return true;
    if (profile == 1 && (setgroups(0, nullptr) || setgid(2001) || setuid(1000))) return false;
    return ker::process::prctl(static_cast<int>(Control::SET_NO_NEW_PRIVILEGES), 1, 0, 0, 0) == 0 &&
           ker::process::prctl(static_cast<int>(Control::DROP_AUTHORITY_CEILING), 0, 0, 0, 0) == 0;
}
int after(unsigned profile, unsigned mode) {
    failures = 0;
    check(getuid() == (profile == 1 ? 1000 : 0) && geteuid() == getuid(), "identity");
    struct sigaction ignore{}, handler{}, term{};
    bool ignored = sigaction(SIGUSR1, nullptr, &ignore) == 0 && ignore.sa_handler == SIG_IGN;
    check(ignored && !(ignore.sa_flags & SA_ONSTACK), "ignored-preserved-without-old-stack");
    check(sigaction(SIGUSR2, nullptr, &handler) == 0 && handler.sa_handler == SIG_DFL, "caught-reset");
    check(sigaction(SIGTERM, nullptr, &term) == 0 && term.sa_handler == SIG_DFL, "handler-exec-reset");
    sigset_t mask{}, pending{};
    check(sigprocmask(SIG_SETMASK, nullptr, &mask) == 0 && sigismember(&mask, SIGUSR2) == 1 &&
              sigismember(&mask, SIGTERM) == (mode == 1 ? 1 : 0),
          "blocked-mask-preserved");
    bool queued = sigpending(&pending) == 0 && sigismember(&pending, SIGUSR2) == 1;
    check(queued, "pending-preserved");
    stack_t stack{};
    check(sigaltstack(nullptr, &stack) == 0 && (stack.ss_flags & SS_DISABLE) && !stack.ss_sp && !stack.ss_size, "old-altstack-disabled");
    handler = {};
    handler.sa_handler = caught;
    sigemptyset(&handler.sa_mask);
    sigset_t release{};
    sigemptyset(&release);
    sigaddset(&release, SIGUSR2);
    bool installed = sigaction(SIGUSR2, &handler, nullptr) == 0 && sigprocmask(SIG_UNBLOCK, &release, nullptr) == 0;
    for (unsigned i = 0; installed && queued && !delivered && i < 1000; ++i) usleep(1000);
    check(installed && delivered == 1, "pending-delivered-in-new-image");
    check(ignored && raise(SIGUSR1) == 0, "ignored-signal-survives");
    return failures ? 1 : 0;
}
int before(unsigned profile, unsigned mode) {
    if (!drop(profile)) return 2;
    struct sigaction ignore{}, handler{}, term{};
    ignore.sa_handler = SIG_IGN;
    ignore.sa_flags = SA_ONSTACK;
    handler.sa_handler = caught;
    handler.sa_flags = SA_ONSTACK | SA_RESTART;
    term.sa_handler = exec_handler;
    term.sa_flags = SA_ONSTACK;
    sigemptyset(&ignore.sa_mask);
    sigemptyset(&handler.sa_mask);
    sigemptyset(&term.sa_mask);
    stack_t stack{.ss_sp = alternate, .ss_flags = 0, .ss_size = sizeof(alternate)};
    sigset_t mask{};
    sigemptyset(&mask);
    sigaddset(&mask, SIGUSR2);
    if (sigaction(SIGUSR1, &ignore, nullptr) || sigaction(SIGUSR2, &handler, nullptr) || sigaction(SIGTERM, &term, nullptr) ||
        sigaltstack(&stack, nullptr) || sigprocmask(SIG_SETMASK, &mask, nullptr) || kill(getpid(), SIGUSR2))
        return 3;
    if (mode == 2) {
        pthread_t live_worker;
        if (pthread_create(&live_worker, nullptr, failed_exec_worker, nullptr)) return 6;
        errno = 0;
        char missing[] = "/tmp/goal19-no-exec-image/absent";
        char* args[] = {missing, nullptr};
        check(execv(missing, args) == -1 && errno == ENOENT, "failed-exec-error");
        struct sigaction old{};
        check(sigaction(SIGUSR1, nullptr, &old) == 0 && old.sa_handler == SIG_IGN && (old.sa_flags & SA_ONSTACK), "failed-ignore");
        check(sigaction(SIGUSR2, nullptr, &old) == 0 && old.sa_handler == caught && (old.sa_flags & SA_ONSTACK), "failed-caught");
        stack_t old_stack{};
        check(sigaltstack(nullptr, &old_stack) == 0 && old_stack.ss_sp == stack.ss_sp && old_stack.ss_size == stack.ss_size &&
                  old_stack.ss_flags == 0,
              "failed-altstack");
        sigset_t old_mask{}, pending{};
        check(sigprocmask(SIG_SETMASK, nullptr, &old_mask) == 0 && sigismember(&old_mask, SIGUSR2) == 1, "failed-mask");
        check(sigpending(&pending) == 0 && sigismember(&pending, SIGUSR2) == 1, "failed-pending");
        __atomic_store_n(&failed_exec_release, 1, __ATOMIC_RELEASE);
        void* result = nullptr;
        check(pthread_join(live_worker, &result) == 0 && result == &failed_exec_release, "failed-exec-preserves-live-worker");
        return failures ? 1 : 0;
    }
    char profile_text[16], mode_text[16];
    snprintf(profile_text, sizeof(profile_text), "%u", profile);
    snprintf(mode_text, sizeof(mode_text), "%u", mode);
    char after_flag[] = "--after";
    next_args[0] = image;
    next_args[1] = after_flag;
    next_args[2] = profile_text;
    next_args[3] = mode_text;
    next_args[4] = nullptr;
    if (mode == 1) {
        raise(SIGTERM);
        return 4;
    }
    execv(image, next_args);
    return 5;
}
}  // namespace
int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    image = argv[0];
    if (argc == 4 && !strcmp(argv[1], "--after")) return after(static_cast<unsigned>(atoi(argv[2])), static_cast<unsigned>(atoi(argv[3])));
    if (argc != 1) return 2;
    unsigned failed = 0;
    for (unsigned profile = 0; profile < 3; ++profile) {
        for (unsigned mode = 0; mode < 3; ++mode) {
            pid_t child = fork();
            if (child == 0) _exit(before(profile, mode));
            int status = 0;
            bool ok = child > 0 && waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
            printf("%s case profile=%u mode=%u status=%d\n", ok ? "PASS" : "FAIL", profile, mode, status);
            failed += !ok;
        }
    }
    printf("%s: 9 native exec-signal cases\n", failed ? "FAIL" : "PASS");
    return failed ? 1 : 0;
}
