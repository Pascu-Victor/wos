/* Disposable WOS VM: each case owns one bounded child process. */
#include <errno.h>
#include <grp.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

enum { WRITERS = 4, ROUNDS = 500, TOTAL = WRITERS * ROUNDS };
static int ready, proceed;
static unsigned seen[TOTAL + 1];
static volatile sig_atomic_t delivered;
static pthread_t main_thread;

static void first(int number) {
    (void)number;
    delivered = 1;
}
static void second(int number) {
    (void)number;
    delivered = 2;
}
static int wait_value(int* value, int target) {
    for (int i = 0; i < 5000; ++i) {
        if (__atomic_load_n(value, __ATOMIC_ACQUIRE) == target) return 0;
        usleep(1000);
    }
    return 1;
}
static struct sigaction action(unsigned id) {
    struct sigaction result = {0};
    result.sa_handler = id & 1 ? second : first;
    result.sa_flags = id & 1 ? SA_RESTART : SA_NODEFER;
    sigemptyset(&result.sa_mask);
    for (unsigned i = 0; i < 12; ++i)
        if (id & (1U << i)) sigaddset(&result.sa_mask, (int)i + 20);
    return result;
}
static int decode(const struct sigaction* value) {
    unsigned id = 0;
    for (unsigned i = 0; i < 12; ++i)
        if (sigismember(&value->sa_mask, (int)i + 20) == 1) id |= 1U << i;
    struct sigaction expected = action(id);
    if (id > TOTAL || value->sa_handler != expected.sa_handler || (value->sa_flags & (SA_RESTART | SA_NODEFER)) != expected.sa_flags)
        return -1;
    return (int)id;
}
static void* existing_worker(void* unused) {
    (void)unused;
    __atomic_store_n(&ready, 1, __ATOMIC_RELEASE);
    if (wait_value(&proceed, 1)) return (void*)1;
    struct sigaction old = {0};
    if (sigaction(SIGUSR1, NULL, &old) || decode(&old) != 1) return (void*)1;
    struct sigaction next = action(2);
    if (sigaction(SIGUSR1, &next, &old) || decode(&old) != 1) return (void*)1;
    return NULL;
}
static int shared_case(void) {
    struct sigaction initial = action(0);
    if (sigaction(SIGUSR1, &initial, NULL)) return 1;
    pthread_t thread;
    if (pthread_create(&thread, NULL, existing_worker, NULL) || wait_value(&ready, 1)) return 1;
    struct sigaction next = action(1);
    if (sigaction(SIGUSR1, &next, NULL)) return 1;
    __atomic_store_n(&proceed, 1, __ATOMIC_RELEASE);
    void* result = NULL;
    if (pthread_join(thread, &result) || result) return 1;
    struct sigaction old = {0};
    return sigaction(SIGUSR1, NULL, &old) || decode(&old) != 2;
}
static void* exchange_worker(void* argument) {
    unsigned writer = (unsigned)(uintptr_t)argument;
    __atomic_add_fetch(&ready, 1, __ATOMIC_RELEASE);
    if (wait_value(&proceed, 1)) return (void*)1;
    for (unsigned round = 0; round < ROUNDS; ++round) {
        struct sigaction next = action(writer * ROUNDS + round + 1), old = {0};
        if (sigaction(SIGUSR2, &next, &old)) return (void*)1;
        int id = decode(&old);
        if (id < 0 || __atomic_fetch_add(&seen[id], 1, __ATOMIC_RELAXED)) return (void*)1;
    }
    return NULL;
}
static int exchange_case(void) {
    struct sigaction initial = action(0);
    if (sigaction(SIGUSR2, &initial, NULL)) return 1;
    pthread_t threads[WRITERS];
    for (unsigned i = 0; i < WRITERS; ++i)
        if (pthread_create(&threads[i], NULL, exchange_worker, (void*)(uintptr_t)i)) return 1;
    if (wait_value(&ready, WRITERS)) return 1;
    __atomic_store_n(&proceed, 1, __ATOMIC_RELEASE);
    for (unsigned i = 0; i < WRITERS; ++i) {
        void* result = NULL;
        if (pthread_join(threads[i], &result) || result) return 1;
    }
    struct sigaction final = {0};
    if (sigaction(SIGUSR2, NULL, &final)) return 1;
    int id = decode(&final);
    if (id < 0 || seen[id]++) return 1;
    for (unsigned i = 0; i <= TOTAL; ++i)
        if (seen[i] != 1) return 1;
    return 0;
}
static int fault_case(void) {
    struct sigaction initial = action(0), next = action(1), old = {0};
    if (sigaction(SIGUSR1, &initial, NULL)) return 1;
    long page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0) return 1;
    char* pages = mmap(NULL, (size_t)page_size * 2, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (pages == MAP_FAILED) return 1;
    if (mprotect(pages + page_size, (size_t)page_size, PROT_NONE)) return 1;
    /* First output bytes are writable, but the rest of the ABI action is not. */
    struct sigaction* bad = (struct sigaction*)(pages + page_size - 16);
    errno = 0;
    if (sigaction(SIGUSR1, &next, bad) != -1 || errno != EFAULT) return 1;
    if (sigaction(SIGUSR1, NULL, &old) || decode(&old) != 0) return 1;
    if (munmap(pages, (size_t)page_size * 2)) return 1;
    return 0;
}
static int wait_child(pid_t child, int* status) {
    for (int i = 0; i < 30000; ++i) {
        pid_t result = waitpid(child, status, WNOHANG);
        if (result == child) return 0;
        if (result < 0 && errno != EINTR) return 1;
        usleep(1000);
    }
    kill(child, SIGKILL);
    waitpid(child, status, 0);
    return 1;
}
static int fork_case(void) {
    struct sigaction initial = action(1);
    if (sigaction(SIGUSR1, &initial, NULL)) return 1;
    pid_t child = fork();
    if (child < 0) return 1;
    if (!child) {
        struct sigaction old = {0}, next = action(2);
        _exit(sigaction(SIGUSR1, &next, &old) || decode(&old) != 1);
    }
    int status = 0;
    if (wait_child(child, &status) || !WIFEXITED(status) || WEXITSTATUS(status)) return 1;
    struct sigaction old = {0};
    return sigaction(SIGUSR1, NULL, &old) || decode(&old) != 1;
}
static void* retired_delivery_worker(void* unused) {
    (void)unused;
    __atomic_store_n(&ready, 1, __ATOMIC_RELEASE);
    if (pthread_join(main_thread, NULL)) _exit(1);
    if (kill(getpid(), SIGUSR1)) _exit(1);
    for (int i = 0; i < 5000 && !delivered; ++i) usleep(1000);
    _exit(delivered != 2);
}
static int delivery_case(void) {
    struct sigaction initial = action(0);
    if (sigaction(SIGUSR1, &initial, NULL)) return 1;
    main_thread = pthread_self();
    pthread_t thread;
    if (pthread_create(&thread, NULL, retired_delivery_worker, NULL) || wait_value(&ready, 1)) return 1;
    struct sigaction next = action(1);
    if (sigaction(SIGUSR1, &next, NULL)) return 1;
    pthread_exit(NULL);
}
int main(int argc, char** argv) {
    if (argc == 2 && !strcmp(argv[1], "--drop")) {
        if (setgroups(0, NULL) || setgid(104) || setuid(104)) return 2;
    } else if (argc != 1)
        return 2;
    setvbuf(stdout, NULL, _IOLBF, 0);
    int (*cases[])(void) = {shared_case, exchange_case, fault_case, fork_case, delivery_case};
    const char* names[] = {"existing_worker_shared_disposition", "concurrent_exchange_2000", "fault_leaves_action_unchanged",
                           "fork_inherits_independent_disposition", "retired_owner_uses_latest_disposition"};
    int failures = 0;
    for (unsigned i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        pid_t child = fork();
        if (child < 0) return 2;
        if (!child) _exit(cases[i]());
        int status = 0;
        int failed = wait_child(child, &status) || !WIFEXITED(status) || WEXITSTATUS(status);
        printf("%s uid=%u %s status=%d\n", failed ? "FAIL" : "PASS", (unsigned)getuid(), names[i], status);
        failures += failed;
    }
    return failures != 0;
}
