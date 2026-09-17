/* Disposable WOS VM: bounded checks using only this fixture's children. */
#include <errno.h>
#include <grp.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

static int report_fd;
static int proceed_fd;
static int ready;
static pthread_t main_thread;
static uid_t expected_uid;
static gid_t expected_gid;
static pid_t expected_pid;
static volatile sig_atomic_t caught;

static void notice(int number) {
    (void)number;
    caught = 1;
}

static ssize_t read_one(int fd, char* byte) {
    struct pollfd event = {.fd = fd, .events = POLLIN};
    int result = poll(&event, 1, 5000);
    return result > 0 ? read(fd, byte, 1) : -1;
}

static void* worker(void* unused) {
    __atomic_store_n(&ready, 1, __ATOMIC_RELEASE);
    if (unused != NULL && pthread_join(main_thread, NULL)) return (void*)1;
    char proceed = 0;
    if (read_one(proceed_fd, &proceed) != 1 || proceed != 'G') return (void*)1;
    close(proceed_fd);
    /* The main thread has retired; this is still the process's descriptor. */
    if (getuid() != expected_uid || geteuid() != expected_uid || getgid() != expected_gid || getegid() != expected_gid ||
        getpid() != expected_pid)
        return (void*)1;
    if (write(report_fd, "W", 1) != 1) return (void*)1;
    return NULL;
}

static int run_case(int mode) {
    int with_worker = mode & 1;
    int control = mode >= 2;
    int report[2];
    if (pipe(report)) return 1;
    int proceed[2];
    if (pipe(proceed)) {
        close(report[0]);
        close(report[1]);
        return 1;
    }
    pid_t child = fork();
    if (child < 0) {
        close(report[0]);
        close(report[1]);
        close(proceed[0]);
        close(proceed[1]);
        return 1;
    }
    if (!child) {
        close(report[0]);
        close(proceed[1]);
        proceed_fd = proceed[0];
        report_fd = report[1];
        main_thread = pthread_self();
        expected_uid = getuid();
        expected_gid = getgid();
        expected_pid = getpid();
        pthread_t thread;
        if (with_worker) {
            if (pthread_create(&thread, NULL, worker, control ? NULL : (void*)1)) _exit(2);
            for (int i = 0; i < 5000 && !__atomic_load_n(&ready, __ATOMIC_ACQUIRE); ++i) usleep(1000);
            if (!__atomic_load_n(&ready, __ATOMIC_ACQUIRE)) _exit(4);
        }
        if (write(report_fd, "R", 1) != 1) _exit(3);
        if (control) {
            void* result = NULL;
            if (with_worker && (pthread_join(thread, &result) || result)) _exit(5);
            _exit(0);
        }
        pthread_exit(NULL);
    }
    close(report[1]);
    close(proceed[0]);
    char bytes[2] = {0};
    ssize_t count = read_one(report[0], bytes);
    int failed = count != 1 || bytes[0] != 'R';
    int status = 0;
    if (with_worker) {
        pid_t early = waitpid(child, &status, WNOHANG);
        if (early != 0) failed = 1;
        if (write(proceed[1], "G", 1) != 1) failed = 1;
        count = read_one(report[0], bytes + 1);
        if (count != 1 || bytes[1] != 'W') failed = 1;
    }
    close(report[0]);
    close(proceed[1]);
    pid_t observed = 0;
    for (int i = 0; i < 2000 && observed == 0; ++i) {
        observed = waitpid(child, &status, WNOHANG);
        if (observed < 0 && errno == EINTR) observed = 0;
        if (!observed) usleep(1000);
    }
    if (observed != child || !WIFEXITED(status) || WEXITSTATUS(status)) failed = 1;
    if (observed == 0) {
        /* Never wait without a deadline on the known broken baseline path. */
        (void)kill(child, SIGKILL);
    }
    printf("%s %s %s: bytes=%c%c wait=%ld expected=%ld status=%d\n", failed ? "FAIL" : "PASS",
           control ? "ordinary exit control" : "main pthread_exit",
           with_worker ? "preserves worker FD and final wait" : "publishes sole-thread exit", bytes[0] ? bytes[0] : '-',
           bytes[1] ? bytes[1] : '-', (long)observed, (long)child, status);
    return failed;
}

static void* spawned_worker(void* unused) {
    (void)unused;
    return getpid() == expected_pid && geteuid() == expected_uid && getegid() == expected_gid ? NULL : (void*)1;
}

enum {
    CREATE_AFTER_RETIRE,
    WORKER_PROCESS_EXIT,
    CAUGHT_SIGNAL,
    BLOCKED_SIGNAL,
    TERMINATE,
    KILL,
    STOP_CONTINUE,
    CHILD_WAIT,
    CHILD_REPARENT,
    RECIPIENT_RETIREMENT
};
static int scenario;
static pid_t bounded_wait(pid_t child, int* status, int flags);
static int race_ready, race_go, race_deliveries;

static void race_notice(int number) {
    (void)number;
    __atomic_add_fetch(&race_deliveries, 1, __ATOMIC_RELAXED);
}

static void* retiring_recipient(void* unused) {
    (void)unused;
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGUSR1);
    if (pthread_sigmask(SIG_UNBLOCK, &mask, NULL)) return (void*)1;
    __atomic_store_n(&race_ready, 1, __ATOMIC_RELEASE);
    while (!__atomic_load_n(&race_go, __ATOMIC_ACQUIRE)) __asm__ volatile("pause");
    return NULL;
}

static int race_recipient_retirement(void) {
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGUSR1);
    for (int round = 0; round < 100; ++round) {
        if (pthread_sigmask(SIG_BLOCK, &mask, NULL)) return 1;
        __atomic_store_n(&race_ready, 0, __ATOMIC_RELAXED);
        __atomic_store_n(&race_go, 0, __ATOMIC_RELAXED);
        pthread_t transient;
        if (pthread_create(&transient, NULL, retiring_recipient, NULL)) return 1;
        for (int spin = 0; spin < 1000 && !__atomic_load_n(&race_ready, __ATOMIC_ACQUIRE); ++spin) usleep(1000);
        if (!__atomic_load_n(&race_ready, __ATOMIC_ACQUIRE)) return 1;
        __atomic_store_n(&race_go, 1, __ATOMIC_RELEASE);
        if (kill(getpid(), SIGUSR1)) return 1;
        void* result = NULL;
        if (pthread_join(transient, &result) || result || pthread_sigmask(SIG_UNBLOCK, &mask, NULL)) return 1;
        for (int spin = 0; spin < 1000 && __atomic_load_n(&race_deliveries, __ATOMIC_RELAXED) == round; ++spin) usleep(1000);
        if (__atomic_load_n(&race_deliveries, __ATOMIC_RELAXED) != round + 1) {
            fprintf(stderr, "recipient retirement round=%d deliveries=%d\n", round, __atomic_load_n(&race_deliveries, __ATOMIC_RELAXED));
            return 1;
        }
    }
    return 0;
}

static void* worker_failed(char step) {
    (void)write(report_fd, &step, 1);
    return (void*)1;
}

static void* controlled_worker(void* unused) {
    (void)unused;
    if (pthread_join(main_thread, NULL)) return (void*)1;
    if (caught) return worker_failed('i');
    struct sigaction action = {0};
    action.sa_handler = scenario == RECIPIENT_RETIREMENT ? race_notice : notice;
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGUSR1, &action, NULL)) return (void*)1;
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGUSR1);
    if (scenario == BLOCKED_SIGNAL && pthread_sigmask(SIG_BLOCK, &mask, NULL)) return (void*)1;
    if (write(report_fd, "J", 1) != 1) return (void*)1;
    if (scenario == WORKER_PROCESS_EXIT) _exit(37);
    if (scenario == CHILD_REPARENT) return NULL;
    if (scenario == CREATE_AFTER_RETIRE) {
        for (int i = 0; i < 32; ++i) {
            pthread_t thread;
            void* result = NULL;
            if (pthread_create(&thread, NULL, spawned_worker, NULL) || pthread_join(thread, &result) || result) return (void*)1;
        }
    } else if (scenario == RECIPIENT_RETIREMENT) {
        if (race_recipient_retirement()) return worker_failed('r');
    } else if (scenario == CHILD_WAIT) {
        pid_t child = fork();
        if (!child) _exit(getppid() == expected_pid && geteuid() == expected_uid ? 23 : 24);
        int status = 0;
        if (child < 0 || bounded_wait(child, &status, 0) != child || !WIFEXITED(status) || WEXITSTATUS(status) != 23)
            return worker_failed('f');
    } else {
        if (scenario == BLOCKED_SIGNAL) {
            char proceed = 0;
            errno = 0;
            ssize_t received = read_one(proceed_fd, &proceed);
            if (received != 1 || proceed != 'U') {
                fprintf(stderr, "worker gate fd=%d read=%ld byte=%d errno=%d\n", proceed_fd, (long)received, proceed, errno);
                return worker_failed('p');
            }
            if (caught) return worker_failed('c');
            sigset_t pending;
            if (sigpending(&pending) || sigismember(&pending, SIGUSR1) != 1) return worker_failed('v');
            if (pthread_sigmask(SIG_UNBLOCK, &mask, NULL)) return worker_failed('u');
        }
        for (int i = 0; i < 5000 && !caught; ++i) usleep(1000);
        if (!caught) return worker_failed('s');
    }
    return write(report_fd, "W", 1) == 1 ? NULL : (void*)1;
}

static pid_t bounded_wait(pid_t child, int* status, int flags) {
    pid_t observed = 0;
    for (int i = 0; i < 3000 && observed == 0; ++i) {
        observed = waitpid(child, status, flags | WNOHANG);
        if (observed < 0 && errno == EINTR) observed = 0;
        if (!observed) usleep(1000);
    }
    return observed;
}

static unsigned char* mapped_bytes;

static void* mapped_worker(void* unused) {
    (void)unused;
    if (pthread_join(main_thread, NULL)) return (void*)1;
    for (int i = 0; i < 4096; ++i) mapped_bytes[i] = (unsigned char)(i * 17 + 3);
    return NULL;
}

static int run_mapped_exit(int with_worker) {
    char path[512];
    const char* directory = getenv("TMPDIR");
    if (!directory) directory = "/tmp";
    int length = snprintf(path, sizeof(path), "%s/goal19-thread-map-XXXXXX", directory);
    if (length < 0 || (size_t)length >= sizeof(path)) return 1;
    int fd = mkstemp(path);
    if (fd < 0) {
        perror("mapped exit mkstemp");
        return 1;
    }
    (void)unlink(path);
    if (ftruncate(fd, 4096)) {
        perror("mapped exit ftruncate");
        close(fd);
        return 1;
    }
    pid_t child = fork();
    if (!child) {
        mapped_bytes = mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (mapped_bytes == MAP_FAILED) _exit(2);
        close(fd);
        if (with_worker) {
            main_thread = pthread_self();
            pthread_t thread;
            if (pthread_create(&thread, NULL, mapped_worker, NULL)) _exit(3);
        } else {
            for (int i = 0; i < 4096; ++i) mapped_bytes[i] = (unsigned char)(i * 17 + 3);
        }
        /* No msync/munmap: final process cleanup must publish the writeback. */
        pthread_exit(NULL);
    }
    int status = 0;
    pid_t observed = child > 0 ? bounded_wait(child, &status, 0) : -1;
    unsigned char bytes[4096];
    int failed = observed != child || child < 0 || !WIFEXITED(status) || WEXITSTATUS(status);
    if (!failed) {
        failed = pread(fd, bytes, sizeof(bytes), 0) != sizeof(bytes);
        for (int i = 0; !failed && i < 4096; ++i) failed |= bytes[i] != (unsigned char)(i * 17 + 3);
    }
    close(fd);
    if (observed == 0) {
        (void)kill(child, SIGKILL);
        (void)bounded_wait(child, &status, 0);
    }
    printf("%s mapped-file writeback before %s wait publication: status=%d\n", failed ? "FAIL" : "PASS",
           with_worker ? "final worker" : "sole main thread", status);
    return failed;
}

static int run_controlled_case(int mode) {
    if (caught || scenario) printf("parent carryover: pid=%ld caught=%d scenario=%d next=%d\n", (long)getpid(), caught, scenario, mode);
    static const char* names[] = {"32 new threads after main retirement",
                                  "worker process exit preserves status37",
                                  "caught signal to retired PID",
                                  "blocked signal retained until worker unmasks",
                                  "TERM to retired PID",
                                  "KILL to retired PID",
                                  "STOP/CONT to retired PID",
                                  "worker forks and waits with retired main owner",
                                  "final thread reparents a live child",
                                  "100 signal deliveries race eligible worker retirement"};
    int report[2];
    if (pipe(report)) return 1;
    int proceed[2];
    if (pipe(proceed)) {
        close(report[0]);
        close(report[1]);
        return 1;
    }
    pid_t child = fork();
    if (child < 0) {
        close(report[0]);
        close(report[1]);
        close(proceed[0]);
        close(proceed[1]);
        return 1;
    }
    if (!child) {
        close(report[0]);
        close(proceed[1]);
        proceed_fd = proceed[0];
        report_fd = report[1];
        main_thread = pthread_self();
        expected_uid = getuid();
        expected_gid = getgid();
        expected_pid = getpid();
        scenario = mode;
        if (mode == CHILD_REPARENT) {
            pid_t descendant = fork();
            if (descendant < 0) _exit(3);
            if (!descendant) {
                char release = 0;
                if (read_one(proceed_fd, &release) != 1 || release != 'G') _exit(4);
                char result = getppid() == 1 && geteuid() == expected_uid ? 'O' : 'e';
                (void)write(report_fd, &result, 1);
                _exit(result == 'O' ? 0 : 5);
            }
        }
        pthread_t thread;
        if (pthread_create(&thread, NULL, controlled_worker, NULL)) _exit(2);
        pthread_exit(NULL);
    }
    close(report[1]);
    close(proceed[0]);
    char byte = 0;
    int failed = read_one(report[0], &byte) != 1 || byte != 'J';
    int status = 0;
    if (!failed && mode == STOP_CONTINUE) {
        failed =
            kill(child, SIGSTOP) || bounded_wait(child, &status, WUNTRACED) != child || !WIFSTOPPED(status) || WSTOPSIG(status) != SIGSTOP;
        if (!failed) failed = kill(child, SIGCONT);
    }
    if (!failed && mode >= CAUGHT_SIGNAL && mode <= STOP_CONTINUE) {
        int signal = mode == TERMINATE ? SIGTERM : mode == KILL ? SIGKILL : SIGUSR1;
        failed = kill(child, signal);
    }
    if (!failed && mode == BLOCKED_SIGNAL) {
        struct pollfd event = {.fd = report[0], .events = POLLIN};
        int result = poll(&event, 1, 30);
        failed = result != 0;
        if (failed) {
            if (result > 0) (void)read_one(report[0], &byte);
            printf("blocked gate poll=%d revents=%d byte=%c errno=%d\n", result, event.revents, byte, errno);
        }
        failed |= write(proceed[1], "U", 1) != 1;
    }
    pid_t observed = 0;
    if (!failed && mode == CHILD_REPARENT) {
        observed = bounded_wait(child, &status, 0);
        failed = observed != child || write(proceed[1], "G", 1) != 1 || read_one(report[0], &byte) != 1 || byte != 'O';
    }
    if (!failed && mode != WORKER_PROCESS_EXIT && mode != TERMINATE && mode != KILL && mode != CHILD_REPARENT) {
        failed = read_one(report[0], &byte) != 1 || byte != 'W';
    }
    close(report[0]);
    close(proceed[1]);
    if (!observed) observed = bounded_wait(child, &status, 0);
    if (observed != child) failed = 1;
    if (mode == TERMINATE || mode == KILL)
        failed |= !WIFSIGNALED(status) || WTERMSIG(status) != (mode == TERMINATE ? SIGTERM : SIGKILL);
    else
        failed |= !WIFEXITED(status) || WEXITSTATUS(status) != (mode == WORKER_PROCESS_EXIT ? 37 : 0);
    if (observed == 0) {
        (void)kill(child, SIGKILL);
        (void)bounded_wait(child, &status, 0);
    }
    printf("%s %s: byte=%c wait=%ld expected=%ld status=%d\n", failed ? "FAIL" : "PASS", names[mode], byte, (long)observed, (long)child,
           status);
    return failed;
}

int main(int argc, char** argv) {
    setvbuf(stdout, NULL, _IOLBF, 0);
    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) return 2;
    int drop = 0, single = -1, signals = 0;
    for (int i = 1; i < argc; ++i) {
        if (!strcmp(argv[i], "--drop"))
            drop = 1;
        else if (!strcmp(argv[i], "--signals"))
            signals = 1;
        else if (!strcmp(argv[i], "--case") && i + 1 < argc)
            single = atoi(argv[++i]);
        else
            return 2;
    }
    if (drop && (setgroups(0, NULL) || setgid(104) || setuid(104))) return 2;
    if (signals) {
        int failed = run_controlled_case(CAUGHT_SIGNAL);
        return !!(failed + run_controlled_case(BLOCKED_SIGNAL));
    }
    if (single >= CREATE_AFTER_RETIRE && single <= RECIPIENT_RETIREMENT) return run_controlled_case(single);
    int failed = 0;
    for (int mode = 0; mode < 4; ++mode) failed += run_case(mode);
    for (int mode = CREATE_AFTER_RETIRE; mode <= RECIPIENT_RETIREMENT; ++mode) failed += run_controlled_case(mode);
    failed += run_mapped_exit(0);
    failed += run_mapped_exit(1);
    return !!failed;
}
