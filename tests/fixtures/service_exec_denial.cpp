// Historical pre-quiescence EAGAIN baseline, for an older disposable WOS VM.
// Current kernels use service_exec_nosuid for successful multithreaded set-ID exec.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
unsigned ready, release_worker;
void caught(int) {}
void* worker(void*) {
    __atomic_store_n(&ready, 1, __ATOMIC_RELEASE);
    while (!__atomic_load_n(&release_worker, __ATOMIC_ACQUIRE)) usleep(1000);
    return &release_worker;
}
struct ExecAttempt {
    char* image;
    char** argv;
    int result;
    int error;
};
void* attempt_exec(void* opaque) {
    auto* attempt = static_cast<ExecAttempt*>(opaque);
    errno = 0;
    attempt->result = execv(attempt->image, attempt->argv);
    attempt->error = errno;
    return opaque;
}
int run_case(char* image, unsigned profile, bool worker_exec) {
    if (profile && (setgroups(0, nullptr) || setgid(2001) || setuid(1000))) return 90;
    uid_t ur, ue, us, ar, ae, as;
    gid_t gr, ge, gs, br, be, bs;
    if (getresuid(&ur, &ue, &us) || getresgid(&gr, &ge, &gs)) return 91;
    char before[512]{}, after[512]{};
    ssize_t before_size = readlink("/proc/self/exe", before, sizeof(before) - 1);
    if (before_size < 0) {
        printf("FAIL initial exe readlink errno=%d\n", errno);
        return 92;
    }
    int fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
    struct sigaction action{};
    action.sa_handler = caught;
    action.sa_flags = SA_RESTART;
    sigemptyset(&action.sa_mask);
    sigaddset(&action.sa_mask, SIGUSR1);
    sigset_t mask{};
    sigemptyset(&mask);
    sigaddset(&mask, SIGUSR2);
    if (fd < 0 || sigaction(SIGUSR2, &action, nullptr) || sigprocmask(SIG_SETMASK, &mask, nullptr)) return 93;
    pthread_t thread;
    if (pthread_create(&thread, nullptr, worker, nullptr)) return 94;
    for (unsigned i = 0; !__atomic_load_n(&ready, __ATOMIC_ACQUIRE); ++i) {
        if (i == 5000) return 95;
        usleep(1000);
    }
    char unexpected[] = "--unexpected";
    char* args[] = {image, unexpected, nullptr};
    ExecAttempt attempt{image, args, 0, 0};
    if (worker_exec) {
        pthread_t executor;
        void* joined_executor = nullptr;
        if (pthread_create(&executor, nullptr, attempt_exec, &attempt) || pthread_join(executor, &joined_executor) ||
            joined_executor != &attempt)
            return 96;
    } else {
        attempt_exec(&attempt);
    }
    int result = attempt.result;
    int error = attempt.error;
    bool ids = getresuid(&ar, &ae, &as) == 0 && getresgid(&br, &be, &bs) == 0 && ar == ur && ae == ue && as == us && br == gr && be == ge &&
               bs == gs;
    ssize_t after_size = readlink("/proc/self/exe", after, sizeof(after) - 1);
    int after_error = after_size < 0 ? errno : 0;
    bool exe = after_size >= 0 && !strcmp(before, after);
    int flags = fcntl(fd, F_GETFD);
    bool descriptor = flags >= 0 && (flags & FD_CLOEXEC);
    struct sigaction saved{};
    sigset_t saved_mask{};
    bool signals = sigaction(SIGUSR2, nullptr, &saved) == 0 && saved.sa_handler == caught && (saved.sa_flags & SA_RESTART) &&
                   sigismember(&saved.sa_mask, SIGUSR1) == 1 && sigprocmask(SIG_SETMASK, nullptr, &saved_mask) == 0 &&
                   sigismember(&saved_mask, SIGUSR2) == 1;
    __atomic_store_n(&release_worker, 1, __ATOMIC_RELEASE);
    void* joined = nullptr;
    bool alive = pthread_join(thread, &joined) == 0 && joined == &release_worker;
    close(fd);
    bool ok = result == -1 && error == EAGAIN && ids && exe && descriptor && signals && alive;
    if (!ok) {
        printf("FAIL denial result=%d errno=%d ids=%d exe=%d fd=%d signals=%d worker=%d\n", result, error, ids, exe, descriptor, signals,
               alive);
        printf("exe before_size=%zd after_size=%zd after_errno=%d before='%s' after='%s'\n", before_size, after_size, after_error, before,
               after);
    }
    return ok ? 0 : 1;
}
}  // namespace
int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc == 2 && !strcmp(argv[1], "--unexpected")) return 98;
    if (argc != 4 && !(argc == 5 && !strcmp(argv[4], "--worker"))) return 2;
    bool const WORKER_EXEC = argc == 5;
    unsigned failures = 0;
    for (unsigned profile = 0; profile < 2; ++profile) {
        for (unsigned mode = 0; mode < 3; ++mode) {
            for (unsigned round = 0; round < 5; ++round) {
                pid_t child = fork();
                if (child == 0) _exit(run_case(argv[mode + 1], profile, WORKER_EXEC));
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
                printf("%s profile=%u mode=%u round=%u reaped=%d status=%d\n", ok ? "PASS" : "FAIL", profile, mode, round, reaped, status);
                failures += !ok;
            }
        }
    }
    printf("%s: 30 native exec denial cases\n", failures ? "FAIL" : "PASS");
    return failures ? 1 : 0;
}
