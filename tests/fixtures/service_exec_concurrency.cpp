// Bounded native races at the exec/credential/process-lifetime boundary.
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
constexpr unsigned ROUNDS = 6;
constexpr unsigned CASES = 78;
struct Event {
    char kind;
    int value;
};
unsigned ready, go, credential_started, credential_stop, credential_paused, selected_mode;
int event_out;
char* image;
char event_arg[24], profile_arg[24], mode_arg[24], secret_arg[24];
char payload_flag[] = "--payload";
char* payload_args[7];
void event(char kind, int value) {
    Event record{kind, value};
    if (write(event_out, &record, sizeof(record)) != sizeof(record)) _exit(90);
}
[[noreturn]] void parked() {
    for (;;) usleep(1000);
}
void wait_gate() {
    __atomic_fetch_add(&ready, 1, __ATOMIC_RELEASE);
    while (!__atomic_load_n(&go, __ATOMIC_ACQUIRE)) asm volatile("pause");
}
void pause_credentials(bool pause) {
    __atomic_store_n(&credential_stop, pause ? 1U : 0U, __ATOMIC_RELEASE);
    for (unsigned i = 0; __atomic_load_n(&credential_paused, __ATOMIC_ACQUIRE) != (pause ? 1U : 0U); ++i) {
        if (i == 5000) {
            event('F', ETIMEDOUT);
            parked();
        }
        usleep(1000);
    }
}
[[noreturn]] void replace() {
    char before[512]{}, after[512]{};
    if (selected_mode == 4) {
        pause_credentials(true);
        if (readlink("/proc/self/exe", before, sizeof(before) - 1) < 0) _exit(91);
        pause_credentials(false);
    }
    for (unsigned attempt = 0; attempt < 16; ++attempt) {
        execv(image, payload_args);
        int const error = errno;
        if (selected_mode != 4 || error != EAGAIN) {
            event(error == EAGAIN ? 'D' : 'F', error);
            parked();
        }
        // A stale admission may reject before replacing the image. Require
        // unchanged saved identity, executable and CLOEXEC state before retry.
        // Quiesce the mutator only for observation: readlink itself can return
        // EAGAIN during credential churn without any exec state being changed.
        pause_credentials(true);
        uid_t real{}, effective{}, saved{};
        bool const intact = getresuid(&real, &effective, &saved) == 0 && real == 0 && saved == 0 && (effective == 0 || effective == 1000) &&
                            fcntl(atoi(secret_arg), F_GETFD) == FD_CLOEXEC && readlink("/proc/self/exe", after, sizeof(after) - 1) >= 0 &&
                            !strcmp(before, after);
        if (!intact) {
            event('F', 92);
            parked();
        }
        event('D', error);
        // Bound churn as well as retries, then require one successful image.
        if (attempt < 3) pause_credentials(false);
    }
    event('F', EAGAIN);
    parked();
}

void* executor(void*) {
    wait_gate();
    if (selected_mode == 4)
        while (!__atomic_load_n(&credential_started, __ATOMIC_ACQUIRE)) asm volatile("pause");
    replace();
}
void* fresh(void* value) { return value; }
void* competitor(void*) {
    wait_gate();
    if (selected_mode == 4) {
        // Real UID remains root; effective UID changes atomically under the
        // same publication guard as exec. Every published image must have a
        // saved UID matching its effective UID, whichever update won.
        for (;;) {
            if (__atomic_load_n(&credential_stop, __ATOMIC_ACQUIRE)) {
                __atomic_store_n(&credential_paused, 1, __ATOMIC_RELEASE);
                while (__atomic_load_n(&credential_stop, __ATOMIC_ACQUIRE)) usleep(1000);
                __atomic_store_n(&credential_paused, 0, __ATOMIC_RELEASE);
            }
            if (seteuid(1000) || seteuid(0)) {
                event('F', errno);
                parked();
            }
            __atomic_store_n(&credential_started, 1, __ATOMIC_RELEASE);
        }
    }
    for (;;) {
        pthread_t thread = nullptr;
        int error = pthread_create(&thread, nullptr, fresh, &ready);
        // Current THREAD_CREATE reports a rejected owner binding as ENOMEM;
        // NPROC reservation can report EAGAIN. Preserve both native errnos and
        // verify that failure did not publish a pthread handle.
        if ((error == EAGAIN || error == ENOMEM) && thread == nullptr) {
            event('C', error);
            continue;
        }
        void* joined = nullptr;
        if (error || pthread_join(thread, &joined) || joined != &ready) {
            event('F', error);
            parked();
        }
    }
}
bool drop(unsigned profile) {
    using Control = ker::abi::process::CredentialControl;
    if (profile == 0) return true;
    if (profile == 1 && (setgroups(0, nullptr) || setgid(2001) || setuid(1000))) return false;
    return ker::process::prctl(static_cast<int>(Control::SET_NO_NEW_PRIVILEGES), 1, 0, 0, 0) == 0 &&
           ker::process::prctl(static_cast<int>(Control::DROP_AUTHORITY_CEILING), 0, 0, 0, 0) == 0;
}
int payload(int argc, char** argv) {
    if (argc != 6) return 91;
    event_out = atoi(argv[2]);
    unsigned profile = atoi(argv[3]);
    unsigned mode = atoi(argv[4]);
    uid_t real, effective, saved;
    gid_t gr, ge, gs;
    unsigned tid = 0;
    asm volatile("movl %%fs:0x18, %0" : "=r"(tid));
    char path[512]{};
    bool identity = getresuid(&real, &effective, &saved) == 0 && getresgid(&gr, &ge, &gs) == 0 && real == (profile == 1 ? 1000U : 0U) &&
                    saved == effective && (mode == 4 ? (effective == 0 || effective == 1000) : effective == real) &&
                    gr == (profile == 1 ? 2001U : 0U) && ge == gr && gs == gr && getpid() == gettid() &&
                    tid == static_cast<unsigned>(gettid()) && readlink("/proc/self/exe", path, sizeof(path) - 1) >= 0 &&
                    !strcmp(path, argv[0]);
    using Control = ker::abi::process::CredentialControl;
    bool authority = ker::process::prctl(static_cast<int>(Control::GET_NO_NEW_PRIVILEGES), 0, 0, 0, 0) == (profile != 0) &&
                     ker::process::prctl(static_cast<int>(Control::GET_AUTHORITY_CEILING), 0, 0, 0, 0) ==
                         (profile == 0 ? ker::abi::process::CREDENTIAL_AUTHORITY_ALL : 0);
    errno = 0;
    bool closed = fcntl(atoi(argv[5]), F_GETFD) == -1 && errno == EBADF;
    pthread_t thread;
    void* joined = nullptr;
    bool threads = pthread_create(&thread, nullptr, fresh, &ready) == 0 && pthread_join(thread, &joined) == 0 && joined == &ready;
    if (!(identity && authority && closed && threads)) {
        printf("payload failure uid=%u/%u/%u gid=%u/%u/%u pid=%d tid=%d tcb=%u identity=%d authority=%d closed=%d threads=%d exe='%s'\n",
               real, effective, saved, gr, ge, gs, static_cast<int>(getpid()), static_cast<int>(gettid()), tid, identity, authority, closed,
               threads, path);
    }
    event(identity && authority && closed && threads ? 'E' : 'F', getpid());
    return identity && authority && closed && threads ? 0 : 92;
}
int run_case(unsigned profile, unsigned mode, unsigned round) {
    int events[2];
    if (pipe(events)) return 1;
    ready = go = credential_started = credential_stop = credential_paused = 0;
    selected_mode = mode;
    event_out = events[1];
    snprintf(event_arg, sizeof(event_arg), "%d", event_out);
    snprintf(profile_arg, sizeof(profile_arg), "%u", profile);
    snprintf(mode_arg, sizeof(mode_arg), "%u", mode);
    payload_args[0] = image;
    payload_args[1] = payload_flag;
    payload_args[2] = event_arg;
    payload_args[3] = profile_arg;
    payload_args[4] = mode_arg;
    payload_args[5] = secret_arg;
    payload_args[6] = nullptr;
    pid_t child = fork();
    if (child == 0) {
        close(events[0]);
        if (!drop(profile)) _exit(93);
        int secret = open("/dev/null", O_RDONLY | O_CLOEXEC);
        if (secret < 0) _exit(94);
        snprintf(secret_arg, sizeof(secret_arg), "%d", secret);
        unsigned count = mode == 1 ? 1 : 2;
        pthread_t threads[2];
        if (pthread_create(&threads[0], nullptr, executor, nullptr) ||
            (count == 2 && pthread_create(&threads[1], nullptr, mode >= 3 ? competitor : executor, nullptr)))
            _exit(95);
        for (unsigned i = 0; __atomic_load_n(&ready, __ATOMIC_ACQUIRE) != count; ++i) {
            if (i == 5000) _exit(96);
            usleep(1000);
        }
        event('R', child);
        __atomic_store_n(&go, 1, __ATOMIC_RELEASE);
        if (mode == 1) replace();
        if (mode == 2) {
            // Vary the exit point without pretending to force a kernel queue
            // or claim boundary. Both exit and successful exec are legal wins.
            constexpr unsigned DELAYS[ROUNDS] = {0, 500, 2000, 8000, 20000, 50000};
            if (DELAYS[round]) usleep(DELAYS[round]);
            _exit(73);
        }
        parked();
    }
    close(events[1]);
    bool reaped = false, started = false, drained = false, bad = child < 0;
    int status = 0, first_error = 0;
    unsigned images = 0, denied = 0, create_denied = 0;
    // Observe events and reap together so an exit race cannot strand the test
    // waiting for a payload that was legitimately cancelled.
    for (unsigned i = 0; child > 0 && i < 1500; ++i) {
        pollfd descriptor{drained ? -1 : events[0], POLLIN, 0};
        int result = poll(&descriptor, 1, 10);
        if (result > 0 && (descriptor.revents & POLLIN)) {
            Event record{};
            ssize_t const COUNT = read(events[0], &record, sizeof(record));
            if (COUNT == 0) {
                drained = true;
            } else if (COUNT != sizeof(record)) {
                bad = true;
                first_error = static_cast<int>(COUNT);
                break;
            } else if (record.kind == 'R')
                started = true;
            else if (record.kind == 'E') {
                ++images;
                bad |= record.value != child;
            } else if (record.kind == 'D')
                ++denied;
            else if (record.kind == 'C')
                ++create_denied;
            else {
                bad = true;
                if (!first_error) first_error = record.value;
            }
        }
        if (!reaped) {
            pid_t result = waitpid(child, &status, WNOHANG);
            if (result == child)
                reaped = true;
            else if (result < 0 && errno != EINTR)
                break;
        }
        if (reaped && !(descriptor.revents & POLLIN)) break;
        if (!reaped && (descriptor.revents & POLLHUP)) usleep(10000);
    }
    if (child > 0 && !reaped) {
        kill(child, SIGKILL);
        // Keep cleanup bounded too: a hung exit is evidence, not a reason for
        // this observer to block indefinitely in a second waitpid.
        for (unsigned i = 0; i < 500; ++i) {
            if (waitpid(child, &status, WNOHANG) == child) break;
            usleep(1000);
        }
    }
    close(events[0]);
    bool normal = WIFEXITED(status) && WEXITSTATUS(status) == 0 && images == 1;
    bool exit_won = mode == 2 && WIFEXITED(status) && WEXITSTATUS(status) == 73 && images == 0;
    bool ok = reaped && started && !bad && (normal || exit_won);
    printf("%s profile=%u mode=%u round=%u started=%d images=%u denied=%u create_denied=%u bad=%d first_error=%d reaped=%d status=%d\n",
           ok ? "PASS" : "FAIL", profile, mode, round, started, images, denied, create_denied, bad, first_error, reaped, status);
    return !ok;
}
}  // namespace
int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc > 1 && !strcmp(argv[1], "--payload")) return payload(argc, argv);
    if (argc != 2) return 2;
    image = argv[1];
    for (unsigned profile = 0; profile < 3; ++profile)
        for (unsigned mode = 0; mode < 4; ++mode)
            for (unsigned round = 0; round < ROUNDS; ++round)
                if (run_case(profile, mode, round)) return 1;
    for (unsigned round = 0; round < ROUNDS; ++round)
        if (run_case(0, 4, round)) return 1;
    printf("PASS: %u native exec-concurrency cases\n", CASES);
    return 0;
}
