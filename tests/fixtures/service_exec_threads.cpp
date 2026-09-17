// Disposable VM regression: the observer stays outside the execing process.
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
#include <sys/wait.h>
#include <unistd.h>

namespace {
struct Event {
    char kind;
    int pid;
    int image_matches;
    int cloexec_closed;
    int fresh_thread_ok;
    int tid_ok;
    int signals_ok;
};
unsigned ready, owner_busy;
int events_out, finish_in, release_in;
char* payload_path;
char event_fd[24], finish_fd[24], cloexec_fd[24], exec_mode[24];
char payload_flag[] = "--payload";
char* payload_args[7];
pthread_t owner_thread;
unsigned mode;
void send_event(char kind, int matches = 0, int closed = 0, int fresh = 0, int signals = 0) {
    unsigned cached_tid = 0;
    asm volatile("movl %%fs:0x18, %0" : "=r"(cached_tid));
    Event event{
        kind,   static_cast<int>(getpid()), matches, closed, fresh, gettid() == getpid() && cached_tid == static_cast<unsigned>(gettid()),
        signals};
    if (write(events_out, &event, sizeof(event)) != sizeof(event)) _exit(91);
}
bool wait_shared(unsigned* field) {
    for (unsigned i = 0; i < 10000; ++i) {
        if (__atomic_load_n(field, __ATOMIC_ACQUIRE)) return true;
        usleep(1000);
    }
    return false;
}
[[noreturn]] void replace_image() {
    execv(payload_path, payload_args);
    send_event('F', errno);
    _exit(92);
}
[[noreturn]] void old_image(char kind) {
    char release = 0;
    if (read(release_in, &release, 1) != 1 || release != 'R') _exit(93);
    send_event(kind);
    // Let the replacement image decide process exit; this old executor must
    // never terminate the process before the observer finishes collecting.
    for (;;) usleep(1000);
}
[[noreturn]] void busy_owner(int = 0) {
    __atomic_store_n(&owner_busy, 1, __ATOMIC_RELEASE);
    for (;;) asm volatile("pause");
}
void* worker(void*) {
    __atomic_store_n(&ready, 1, __ATOMIC_RELEASE);
    if (mode == 0) old_image('W');
    if (mode == 2 && pthread_join(owner_thread, nullptr)) _exit(94);
    if (mode >= 3 && !wait_shared(&owner_busy)) _exit(98);
    sigset_t mask{};
    sigemptyset(&mask);
    sigaddset(&mask, SIGUSR2);
    // WOS kill resolves a positive task ID directly; libc raise targets getpid(),
    // which would signal the unblocked process owner instead of this worker.
    if (sigprocmask(SIG_SETMASK, &mask, nullptr) || kill(gettid(), SIGUSR2)) _exit(101);
    replace_image();
}
bool drop(unsigned profile) {
    using Control = ker::abi::process::CredentialControl;
    if (profile == 0) return true;
    if (profile == 1 && (setgroups(0, nullptr) || setgid(2001) || setuid(1000))) return false;
    return ker::process::prctl(static_cast<int>(Control::SET_NO_NEW_PRIVILEGES), 1, 0, 0, 0) == 0 &&
           ker::process::prctl(static_cast<int>(Control::DROP_AUTHORITY_CEILING), 0, 0, 0, 0) == 0;
}
void* fresh_worker(void* value) { return value; }
int payload(int argc, char** argv) {
    if (argc != 6) return 2;
    events_out = atoi(argv[2]);
    finish_in = atoi(argv[3]);
    char actual[512]{};
    ssize_t size = readlink("/proc/self/exe", actual, sizeof(actual) - 1);
    errno = 0;
    bool closed = fcntl(atoi(argv[4]), F_GETFD) == -1 && errno == EBADF;
    pthread_t fresh;
    void* result = nullptr;
    bool fresh_ok = pthread_create(&fresh, nullptr, fresh_worker, &actual) == 0 && pthread_join(fresh, &result) == 0 && result == &actual;
    sigset_t mask{}, pending{};
    bool const EXPECT_WORKER = atoi(argv[5]) != 0;
    bool const SIGNALS_OK = sigprocmask(SIG_SETMASK, nullptr, &mask) == 0 && sigpending(&pending) == 0 &&
                            sigismember(&mask, SIGUSR1) == 0 && sigismember(&mask, SIGUSR2) == EXPECT_WORKER &&
                            sigismember(&pending, SIGUSR2) == EXPECT_WORKER;
    send_event('E', size >= 0 && !strcmp(actual, argv[0]), closed, fresh_ok, SIGNALS_OK);
    char done = 0;
    if (read(finish_in, &done, 1) != 1 || done != 'D') return 3;
    return 0;
}
int run_case(unsigned profile, unsigned selected_mode) {
    ready = 0;
    owner_busy = 0;
    int events[2], finish[2], release[2];
    if (pipe(events) || pipe(finish) || pipe(release)) return 2;
    release_in = release[0];
    events_out = events[1];
    finish_in = finish[0];
    snprintf(event_fd, sizeof(event_fd), "%d", events_out);
    snprintf(finish_fd, sizeof(finish_fd), "%d", finish_in);
    payload_args[0] = payload_path;
    payload_args[1] = payload_flag;
    payload_args[2] = event_fd;
    payload_args[3] = finish_fd;
    payload_args[4] = cloexec_fd;
    snprintf(exec_mode, sizeof(exec_mode), "%u", selected_mode);
    payload_args[5] = exec_mode;
    payload_args[6] = nullptr;
    mode = selected_mode;
    pid_t child = fork();
    if (child == 0) {
        close(release[1]);
        close(events[0]);
        close(finish[1]);
        if (!drop(profile)) _exit(95);
        int secret = open("/dev/null", O_RDONLY | O_CLOEXEC);
        if (secret < 0) _exit(97);
        snprintf(cloexec_fd, sizeof(cloexec_fd), "%d", secret);
        owner_thread = pthread_self();
        pthread_t thread;
        if (pthread_create(&thread, nullptr, worker, nullptr) || !wait_shared(&ready)) _exit(96);
        if (mode == 0) replace_image();
        if (mode == 2) pthread_exit(nullptr);
        if (mode == 3) busy_owner();
        if (mode == 4) {
            if (signal(SIGUSR1, busy_owner) == SIG_ERR || raise(SIGUSR1)) _exit(99);
            _exit(100);
        }
        old_image('M');
    }
    close(release[0]);
    close(events[1]);
    close(finish[0]);
    bool replacement = false, old_survived = false, correct_pid = false, correct_image = false, closed = false, fresh_ok = false,
         tid_ok = false, signals_ok = false;
    int polls = 0;
    for (; child > 0 && polls < 100; ++polls) {
        pollfd descriptor{events[0], POLLIN, 0};
        int result = poll(&descriptor, 1, 100);
        if (result < 0 && errno == EINTR) continue;
        if (result < 0 || (descriptor.revents & (POLLERR | POLLNVAL))) break;
        if (!(descriptor.revents & POLLIN)) {
            if (descriptor.revents & POLLHUP) break;
            continue;
        }
        Event event{};
        if (read(events[0], &event, sizeof(event)) != sizeof(event)) break;
        if (event.kind == 'E') {
            replacement = true;
            correct_pid = event.pid == child;
            tid_ok = event.tid_ok;
            signals_ok = event.signals_ok;
            correct_image = event.image_matches != 0;
            closed = event.cloexec_closed != 0;
            fresh_ok = event.fresh_thread_ok != 0;
            char wake = 'R';
            if (write(release[1], &wake, 1) != 1) replacement = false;
            break;
        }
        if (event.kind == 'F') break;
    }
    if (replacement) {
        // The old executor was ready before exec. Give it an explicit release
        // after new-image admission and observe while the payload stays alive.
        for (unsigned i = 0; i < 10; ++i) {
            pollfd descriptor{events[0], POLLIN, 0};
            if (poll(&descriptor, 1, 50) > 0 && (descriptor.revents & POLLIN)) {
                Event event{};
                if (read(events[0], &event, sizeof(event)) == sizeof(event) && (event.kind == 'M' || event.kind == 'W'))
                    old_survived = true;
            }
        }
        char done = 'D';
        if (write(finish[1], &done, 1) != 1) replacement = false;
    }
    bool reaped = false;
    int status = 0;
    for (unsigned i = 0; child > 0 && i < 5000; ++i) {
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
    close(events[0]);
    close(finish[1]);
    close(release[1]);
    bool ok = replacement && !old_survived && correct_pid && correct_image && closed && fresh_ok && tid_ok && signals_ok && reaped &&
              WIFEXITED(status) && WEXITSTATUS(status) == 0;
    printf(
        "%s profile=%u mode=%u replacement=%d old_survived=%d pid_ok=%d image_ok=%d cloexec_closed=%d fresh_thread_ok=%d tid_ok=%d "
        "signals_ok=%d "
        "reaped=%d "
        "status=%d\n",
        ok ? "PASS" : "FAIL", profile, mode, replacement, old_survived, correct_pid, correct_image, closed, fresh_ok, tid_ok, signals_ok,
        reaped, status);
    return !ok;
}
}  // namespace
int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc > 1 && !strcmp(argv[1], "--payload")) return payload(argc, argv);
    if (argc != 2) return 2;
    payload_path = argv[1];
    unsigned failed = 0;
    for (unsigned profile = 0; profile < 3; ++profile)
        for (unsigned selected = 0; selected < 5; ++selected) failed += run_case(profile, selected);
    printf("%s: 15 native exec-thread cases\n", failed ? "FAIL" : "PASS");
    return failed ? 1 : 0;
}
