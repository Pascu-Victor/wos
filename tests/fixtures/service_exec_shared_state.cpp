// Race old-image sigaction against main-thread exec in a disposable VM.
#include <errno.h>
#include <fcntl.h>
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
unsigned mutations;
void old_handler(int) {}
void* mutate_actions(void*) {
    struct sigaction action{};
    action.sa_handler = old_handler;
    action.sa_flags = SA_RESTART | SA_ONSTACK;
    sigemptyset(&action.sa_mask);
    sigaddset(&action.sa_mask, SIGUSR1);
    for (;;) {
        if (sigaction(SIGUSR2, &action, nullptr)) _exit(90);
        __atomic_add_fetch(&mutations, 1, __ATOMIC_RELEASE);
    }
}
bool drop(unsigned profile) {
    using Control = ker::abi::process::CredentialControl;
    if (profile == 0) return true;
    if (profile == 1 && (setgroups(0, nullptr) || setgid(2001) || setuid(1000))) return false;
    return ker::process::prctl(static_cast<int>(Control::SET_NO_NEW_PRIVILEGES), 1, 0, 0, 0) == 0 &&
           ker::process::prctl(static_cast<int>(Control::DROP_AUTHORITY_CEILING), 0, 0, 0, 0) == 0;
}
int payload(unsigned profile) {
    struct sigaction action{}, ignored{};
    sigset_t mask{};
    bool reset = sigaction(SIGUSR2, nullptr, &action) == 0 && action.sa_handler == SIG_DFL &&
                 !(action.sa_flags & (SA_RESTART | SA_ONSTACK)) && !sigismember(&action.sa_mask, SIGUSR1);
    bool preserved = sigaction(SIGUSR1, nullptr, &ignored) == 0 && ignored.sa_handler == SIG_IGN &&
                     sigprocmask(SIG_SETMASK, nullptr, &mask) == 0 && sigismember(&mask, SIGUSR2) == 1;
    bool identity = getuid() == (profile == 1 ? 1000 : 0) && geteuid() == getuid();
    if (!reset || !preserved || !identity)
        printf("FAIL payload reset=%d ignored_mask_preserved=%d identity=%d handler=%p flags=%lx\n", reset, preserved, identity,
               reinterpret_cast<void*>(action.sa_handler), action.sa_flags);
    return reset && preserved && identity ? 0 : 1;
}
[[noreturn]] void before(char* image, unsigned profile) {
    if (!drop(profile)) _exit(91);
    struct sigaction ignored{};
    ignored.sa_handler = SIG_IGN;
    sigemptyset(&ignored.sa_mask);
    sigset_t mask{};
    sigemptyset(&mask);
    sigaddset(&mask, SIGUSR2);
    if (sigaction(SIGUSR1, &ignored, nullptr) || sigprocmask(SIG_SETMASK, &mask, nullptr)) _exit(92);
    // Use real CLOEXEC work to expose the post-reset/pre-retirement interval.
    // Leave spare table capacity for pthread and executable-loader operations.
    for (unsigned i = 0; i < 200; ++i)
        if (open("/dev/null", O_RDONLY | O_CLOEXEC) < 0) _exit(93);
    pthread_t worker;
    if (pthread_create(&worker, nullptr, mutate_actions, nullptr)) _exit(94);
    for (unsigned i = 0; __atomic_load_n(&mutations, __ATOMIC_ACQUIRE) < 100; ++i) {
        if (i == 5000) _exit(95);
        usleep(1000);
    }
    char profile_text[16], flag[] = "--payload";
    snprintf(profile_text, sizeof(profile_text), "%u", profile);
    char* args[] = {image, flag, profile_text, nullptr};
    execv(image, args);
    _exit(96);
}
}  // namespace

int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc == 3 && !strcmp(argv[1], "--payload")) return payload(static_cast<unsigned>(atoi(argv[2])));
    if (argc != 1) return 2;
    unsigned failures = 0;
    for (unsigned profile = 0; profile < 3; ++profile) {
        for (unsigned round = 0; round < 10; ++round) {
            pid_t child = fork();
            if (child == 0) before(argv[0], profile);
            bool reaped = false;
            int status = 0;
            for (unsigned i = 0; child > 0 && i < 10000; ++i) {
                pid_t result = waitpid(child, &status, WNOHANG);
                if (result == child) {
                    reaped = true;
                    break;
                }
                if (result < 0 && errno != EINTR) break;
                usleep(1000);
            }
            if (child > 0 && !reaped) {
                kill(child, SIGKILL);
                waitpid(child, &status, 0);
            }
            bool ok = reaped && WIFEXITED(status) && WEXITSTATUS(status) == 0;
            printf("%s profile=%u round=%u reaped=%d status=%d\n", ok ? "PASS" : "FAIL", profile, round, reaped, status);
            failures += !ok;
        }
    }
    printf("%s: 30 native exec shared-state cases\n", failures ? "FAIL" : "PASS");
    return failures ? 1 : 0;
}
