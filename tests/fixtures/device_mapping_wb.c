// Public userspace fixture for --selftest-device-mapping; no private syscalls.
#define _GNU_SOURCE 1
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define MAGIC UINT64_C(0x574f534442574231)
#define PAGE 4096UL
#define WORDS (PAGE / sizeof(uint64_t))
#define BASE UINT64_C(0x5200000000)
#define ADJACENT UINT64_C(0x5400000000)
#define MIXED UINT64_C(0x5600000000)
#define SPARSE UINT64_C(0x5800000000)
enum { CHECK = 1, STORE, REMOVE_GRANT, HOLD, DROP, REVOKE, ASSERT_READS, DONE, RAW_COPY_REFUSAL };
struct record {
    uint64_t magic, version, bytes, base, end, grant_generation, object_generation, page_size;
};
struct request {
    uint64_t magic, command, offset, value, extra, reserved[3];
};
_Static_assert(sizeof(struct record) == 64 && sizeof(struct request) == 64, "fixed test protocol");
static unsigned assertions, failures;

static int expect(int valid, const char* reason) {
    ++assertions;
    if (!valid) {
        ++failures;
        fprintf(stderr, "DEVICE_WB_USER FAIL %s errno=%d\n", reason, errno);
    }
    return valid;
}
#define REQUIRE(valid, reason)                        \
    do {                                              \
        if (!expect((valid), (reason))) goto cleanup; \
    } while (0)

static int command(int fd, uint64_t op, uint64_t offset, uint64_t value, uint64_t extra) {
    struct request request = {.magic = MAGIC, .command = op, .offset = offset, .value = value, .extra = extra};
    ssize_t result;
    do {
        result = write(fd, &request, sizeof(request));
    } while (result < 0 && errno == EINTR);
    return result == (ssize_t)sizeof(request) ? 0 : -1;
}
static int record(int fd, struct record* out) {
    ssize_t result;
    do {
        result = read(fd, out, sizeof(*out));
    } while (result < 0 && errno == EINTR);
    return result == (ssize_t)sizeof(*out) && out->magic == MAGIC && out->version == 1 && out->page_size == PAGE &&
           out->bytes == 160 * PAGE && out->base > 0 && out->base % PAGE == 0 && out->end - out->base == out->bytes &&
           out->grant_generation != 0 && out->object_generation != 0;
}
static void* map(void* address, size_t bytes, int protections, int flags, int fd, uint64_t offset) {
    for (unsigned attempt = 0; attempt < 2000; ++attempt) {
        void* result = mmap(address, bytes, protections, flags, fd, (off_t)offset);
        if (result != MAP_FAILED || errno != EAGAIN) return result;
        usleep(1000);
    }
    return MAP_FAILED;
}
static int protect(void* address, size_t bytes, int protections) {
    for (unsigned attempt = 0; attempt < 2000; ++attempt) {
        int result = mprotect(address, bytes, protections);
        if (result == 0 || errno != EAGAIN) return result;
        usleep(1000);
    }
    return -1;
}
static int unmap(void* address, size_t bytes) {
    for (unsigned attempt = 0; attempt < 2000; ++attempt) {
        int result = munmap(address, bytes);
        if (result == 0 || errno != EAGAIN) return result;
        usleep(1000);
    }
    return -1;
}
static pid_t spawn(void) {
    for (unsigned attempt = 0; attempt < 2000; ++attempt) {
        pid_t child = fork();
        if (child >= 0 || errno != EAGAIN) return child;
        usleep(1000);
    }
    return -1;
}
static int reap(pid_t child) {
    int status = 0;
    pid_t result;
    do {
        result = waitpid(child, &status, 0);
    } while (result < 0 && errno == EINTR);
    int error = result < 0 ? errno : 0;
    int passed = result == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
    if (!passed) {
        fprintf(stderr, "DEVICE_WB_USER CHILD_WAIT expected_pid=%ld actual_pid=%ld status=0x%x exit=%d signal=%d wait_errno=%d\n",
                (long)child, (long)result, status, WIFEXITED(status) ? WEXITSTATUS(status) : -1, WIFSIGNALED(status) ? WTERMSIG(status) : 0,
                error);
        fflush(stderr);
    }
    errno = error;
    return passed;
}
static int reap_revoked(pid_t child) {
    int status = 0;
    pid_t result;
    do {
        result = waitpid(child, &status, 0);
    } while (result < 0 && errno == EINTR);
    return result == child && ((WIFEXITED(status) && WEXITSTATUS(status) == 0) || (WIFSIGNALED(status) && WTERMSIG(status) == SIGSEGV));
}
static void revoked_handler(int signal_number) { _exit(signal_number == SIGSEGV ? 0 : 96); }
static uint64_t load(volatile uint64_t* word) { return __atomic_load_n(word, __ATOMIC_ACQUIRE); }
static void store(volatile uint64_t* word, uint64_t value) { __atomic_store_n(word, value, __ATOMIC_RELEASE); }

int main(int argc, char** argv) {
    if (argc == 3 && !strcmp(argv[1], "--exec-probe")) {
        void* old = (void*)(uintptr_t)strtoull(argv[2], NULL, 16);
        errno = 0;
        int result = protect(old, PAGE, PROT_READ);
        int error = errno;
        if (result == -1 && error == ENOMEM) return 0;
        fprintf(stderr, "DEVICE_WB_USER EXEC_PROBE pid=%ld va=%p protect=%d errno=%d\n", (long)getpid(), old, result, error);
        fflush(stderr);
        return 91;
    }
    if (geteuid() != 0) return 2;
    alarm(180);
    int fd = -1, control = -1, readonly = -1;
    int ready[2] = {-1, -1};
    pid_t child = -1;
    struct record a = {0}, b = {0}, ro = {0};
    volatile uint64_t* memory = MAP_FAILED;
    void* sentinel = MAP_FAILED;
    int adjacent_live = 0, mixed_live = 0, sparse_live = 0;
    int completed = 0;
    fd = open("/dev/ktest_device_mapping_wb", O_RDWR | O_CLOEXEC);
    control = open("/dev/ktest_device_mapping_wb", O_RDWR | O_CLOEXEC);
    readonly = open("/dev/ktest_device_mapping_wb", O_RDONLY | O_CLOEXEC);
    REQUIRE(fd >= 0 && control >= 0 && readonly >= 0, "genuine user process opens retained endpoint");
    REQUIRE(record(fd, &a) && record(control, &b) && record(readonly, &ro) && a.base != b.base && a.base != ro.base,
            "distinct File Clients receive disjoint intervals");
    memory = map((void*)(uintptr_t)BASE, 130 * PAGE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, a.base);
    REQUIRE(memory != MAP_FAILED && (uintptr_t)memory == BASE, "public130-page mapping crosses64-page boundary");
    for (unsigned page = 0; page < 130; ++page) REQUIRE(load(memory + page * WORDS) == 0, "real initialized system pages");
    REQUIRE(command(control, RAW_COPY_REFUSAL, 0, 0, 0) == 0, "real raw/WKI-compatible copier refuses before any private target leaf");
    errno = 0;
    REQUIRE(map(NULL, PAGE, PROT_WRITE, MAP_SHARED, readonly, ro.base) == MAP_FAILED && errno == EACCES, "read-only ceiling true errno");
    errno = 0;
    REQUIRE(map(NULL, PAGE, PROT_READ, MAP_SHARED, control, a.base) == MAP_FAILED && errno == ENOENT, "foreign interval refusal");
    errno = 0;
    REQUIRE(map(NULL, PAGE, PROT_READ, MAP_PRIVATE, fd, a.base) == MAP_FAILED && errno == EINVAL, "private-copy refusal");
    errno = 0;
    REQUIRE(map(NULL, PAGE, PROT_EXEC, MAP_SHARED, fd, a.base) == MAP_FAILED && errno == EINVAL, "executable device-map refusal");
    store(memory, UINT64_C(0x1122334455667788));
    REQUIRE(command(control, CHECK, 0, UINT64_C(0x1122334455667788), 0) == 0, "user write visible in actual driver backing");
    REQUIRE(command(control, STORE, PAGE, UINT64_C(0x8877665544332211), 0) == 0 && load(memory + WORDS) == UINT64_C(0x8877665544332211),
            "driver write visible in actual user mapping");
    REQUIRE(command(fd, REMOVE_GRANT, a.base, a.end, a.grant_generation) == 0, "remove exact Client grant");
    errno = 0;
    REQUIRE(
        map(NULL, PAGE, PROT_READ, MAP_SHARED, fd, a.base) == MAP_FAILED && errno == ENOENT && load(memory) == UINT64_C(0x1122334455667788),
        "stale token refuses while existing mapping survives");
    REQUIRE(close(fd) == 0, "close descriptor retains mapped open description");
    fd = -1;
    REQUIRE(load(memory) == UINT64_C(0x1122334455667788), "real mapping survives FD close");
    sentinel = map(NULL, 4 * PAGE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    REQUIRE(sentinel != MAP_FAILED, "occupied sentinel allocation");
    memset(sentinel, 0xA7, 4 * PAGE);
    errno = 0;
    REQUIRE(map(sentinel, PAGE, PROT_READ, MAP_SHARED | MAP_FIXED, control, b.base) == MAP_FAILED && errno == EOPNOTSUPP &&
                ((unsigned char*)sentinel)[0] == 0xA7 && ((unsigned char*)sentinel)[4 * PAGE - 1] == 0xA7,
            "occupied MAP_FIXED refusal preserves sentinel");
    REQUIRE(unmap(sentinel, 4 * PAGE) == 0, "sentinel cleanup");
    sentinel = MAP_FAILED;
    REQUIRE(protect((void*)(uintptr_t)(BASE + 10 * PAGE), 10 * PAGE, PROT_NONE) == 0 &&
                protect((void*)(uintptr_t)(BASE + 12 * PAGE), 2 * PAGE, PROT_READ | PROT_WRITE) == 0,
            "partial PROT_NONE and subset restoration");
    child = spawn();
    REQUIRE(child >= 0, "genuine fork of device PTEs and partial protections");
    if (child == 0) {
        errno = 0;
        int restored = protect((void*)(uintptr_t)(BASE + 10 * PAGE), 10 * PAGE, PROT_READ | PROT_WRITE);
        int restore_error = errno;
        if (restored != 0) {
            fprintf(stderr, "DEVICE_WB_USER CHILD_PROTECT pid=%ld va=%llx bytes=%lu result=%d errno=%d\n", (long)getpid(),
                    (unsigned long long)(BASE + 10 * PAGE), 10 * PAGE, restored, restore_error);
            fflush(stderr);
            _exit(92);
        }
        store(memory + 96 * WORDS, UINT64_C(0xAABBCCDDEEFF0123));
        close(control);
        close(readonly);
        char address[32];
        snprintf(address, sizeof(address), "%llx", (unsigned long long)BASE);
        execl(argv[0], argv[0], "--exec-probe", address, (char*)NULL);
        int exec_error = errno;
        fprintf(stderr, "DEVICE_WB_USER CHILD_EXEC pid=%ld path=%s errno=%d\n", (long)getpid(), argv[0], exec_error);
        fflush(stderr);
        _exit(93);
    }
    REQUIRE(reap(child), "exec drops actual child root device bindings");
    child = -1;
    REQUIRE(load(memory + 96 * WORDS) == UINT64_C(0xAABBCCDDEEFF0123) &&
                command(control, CHECK, 96 * PAGE, UINT64_C(0xAABBCCDDEEFF0123), 0) == 0,
            "fork preserves true shared physical visibility");
    REQUIRE(protect((void*)(uintptr_t)(BASE + 10 * PAGE), 10 * PAGE, PROT_READ | PROT_WRITE) == 0, "parent protection restoration");
    REQUIRE(unmap((void*)(uintptr_t)(BASE + 32 * PAGE), 8 * PAGE) == 0, "partial unmap preserves surrounding bindings");
    errno = 0;
    REQUIRE(protect((void*)(uintptr_t)(BASE + 32 * PAGE), PAGE, PROT_READ) == -1 && errno == ENOMEM,
            "mprotect actual removed hole gives ENOMEM");
    REQUIRE(map((void*)(uintptr_t)(ADJACENT + 64 * PAGE), 64 * PAGE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, control,
                b.base + 64 * PAGE) != MAP_FAILED,
            "publish high adjacent view first");
    adjacent_live = 1;
    REQUIRE(map((void*)(uintptr_t)ADJACENT, 64 * PAGE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, control, b.base) != MAP_FAILED,
            "publish low adjacent view second");
    REQUIRE(protect((void*)(uintptr_t)(ADJACENT + 32 * PAGE), 64 * PAGE, PROT_READ) == 0 &&
                protect((void*)(uintptr_t)(ADJACENT + 32 * PAGE), 64 * PAGE, PROT_READ | PROT_WRITE) == 0,
            "combined partial protect sorts reversed adjacent bindings");
    REQUIRE(unmap((void*)(uintptr_t)(ADJACENT + 48 * PAGE), 32 * PAGE) == 0 && unmap((void*)(uintptr_t)ADJACENT, 48 * PAGE) == 0 &&
                unmap((void*)(uintptr_t)(ADJACENT + 80 * PAGE), 48 * PAGE) == 0,
            "cross-boundary partial unmap and surviving fragment cleanup");
    adjacent_live = 0;
    REQUIRE(map((void*)(uintptr_t)MIXED, 2 * PAGE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE | MAP_FIXED, -1,
                0) != MAP_FAILED &&
                unmap((void*)(uintptr_t)MIXED, PAGE) == 0 &&
                map((void*)(uintptr_t)MIXED, PAGE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, control, b.base) != MAP_FAILED,
            "prepare actual adjacent device and lazy anonymous intervals");
    mixed_live = 1;
    errno = 0;
    REQUIRE(protect((void*)(uintptr_t)MIXED, 2 * PAGE, PROT_READ) == -1 && errno == EOPNOTSUPP,
            "mixed lazy/device protect refuses before effects");
    store((volatile uint64_t*)(uintptr_t)MIXED, 0x715);
    store((volatile uint64_t*)(uintptr_t)(MIXED + PAGE), 0x816);
    errno = 0;
    REQUIRE(unmap((void*)(uintptr_t)MIXED, 2 * PAGE) == -1 && errno == EOPNOTSUPP && load((volatile uint64_t*)(uintptr_t)MIXED) == 0x715 &&
                load((volatile uint64_t*)(uintptr_t)(MIXED + PAGE)) == 0x816,
            "mixed remove refusal preserves both live mappings");
    REQUIRE(unmap((void*)(uintptr_t)MIXED, PAGE) == 0 && unmap((void*)(uintptr_t)(MIXED + PAGE), PAGE) == 0, "mixed interval cleanup");
    mixed_live = 0;
    REQUIRE(map((void*)(uintptr_t)SPARSE, 2 * PAGE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, control, b.base) != MAP_FAILED,
            "sparse left mapping");
    sparse_live = 1;
    REQUIRE(map((void*)(uintptr_t)(SPARSE + 3 * PAGE), 2 * PAGE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, control,
                b.base + 3 * PAGE) != MAP_FAILED,
            "sparse right mapping");
    errno = 0;
    REQUIRE(protect((void*)(uintptr_t)SPARSE, 5 * PAGE, PROT_READ) == -1 && errno == ENOMEM,
            "combined sparse device interval refuses hole without effects");
    store((volatile uint64_t*)(uintptr_t)SPARSE, 0x917);
    store((volatile uint64_t*)(uintptr_t)(SPARSE + 3 * PAGE), 0xA18);
    REQUIRE(unmap((void*)(uintptr_t)SPARSE, 2 * PAGE) == 0 && unmap((void*)(uintptr_t)(SPARSE + 3 * PAGE), 2 * PAGE) == 0,
            "sparse writable fragments survive refusal");
    sparse_live = 0;
    REQUIRE(pipe(ready) == 0, "actual child revoke readiness pipe");
    child = spawn();
    REQUIRE(child >= 0, "genuine child retains second root for revoke");
    if (child == 0) {
        char byte = 'R';
        close(ready[0]);
        close(control);
        close(readonly);
        void* signal_stack = map(NULL, 4 * PAGE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (signal_stack == MAP_FAILED) _exit(94);
        memset(signal_stack, 0, 4 * PAGE);
        stack_t alternate = {.ss_sp = signal_stack, .ss_size = 4 * PAGE};
        struct sigaction action = {0};
        action.sa_handler = revoked_handler;
        action.sa_flags = SA_ONSTACK;
        sigemptyset(&action.sa_mask);
        if (sigaltstack(&alternate, NULL) || sigaction(SIGSEGV, &action, NULL)) _exit(95);
        alarm(60);
        volatile uint64_t warmed = load(memory);
        if (write(ready[1], &byte, 1) != 1) _exit(94);
        for (;;) {
            warmed ^= load(memory);
            __asm__ volatile("pause");
        }
    }
    close(ready[1]);
    ready[1] = -1;
    char byte;
    REQUIRE(read(ready[0], &byte, 1) == 1 && byte == 'R', "child warmed actual TLB and is busy-reading device data");
    REQUIRE(command(control, HOLD, 0, BASE, 0) == 0, "retain actual device frame independently of root");
    printf("DEVICE_WB_USER EXPECT_REVOKE_FAULT pid=%ld va=%llx\n", (long)child, (unsigned long long)BASE);
    fflush(stdout);
    errno = 0;
    REQUIRE(command(control, REVOKE, (uint64_t)child, BASE, 0) == -1 && errno == EAGAIN, "actual revoke waits retained frame export");
    REQUIRE(command(control, DROP, 0, 0, 0) == 0, "actual IRQ-safe retained frame return");
    int revoked = 0;
    for (unsigned attempt = 0; attempt < 5000; ++attempt) {
        if (command(control, REVOKE, (uint64_t)child, BASE, 0) == 0) {
            revoked = 1;
            break;
        }
        if (errno != EAGAIN) break;
        usleep(1000);
    }
    REQUIRE(revoked, "actual multi-root CPU revoke and callback drain complete");
    errno = 0;
    REQUIRE(protect((void*)(uintptr_t)BASE, PAGE, PROT_READ) == -1 && errno == ENOMEM, "parent revoked PTEs are holes");
    errno = 0;
    REQUIRE(map(NULL, PAGE, PROT_READ, MAP_SHARED, control, b.base) == MAP_FAILED && errno == ENODEV, "revoked object cannot republish");
    REQUIRE(reap_revoked(child), "child busy-read loses access only through expected SIGSEGV");
    child = -1;
    REQUIRE(command(control, ASSERT_READS, 0, 3, 0) == 0, "mmap never invokes device read callback");
    completed = 1;
cleanup:
    if (child > 0) {
        kill(child, SIGKILL);
        waitpid(child, NULL, 0);
    }
    for (unsigned index = 0; index < 2; ++index) {
        if (ready[index] >= 0) close(ready[index]);
    }
    if (memory != MAP_FAILED) unmap((void*)(uintptr_t)BASE, 130 * PAGE);
    if (sentinel != MAP_FAILED) unmap(sentinel, 4 * PAGE);
    if (adjacent_live) unmap((void*)(uintptr_t)ADJACENT, 128 * PAGE);
    if (mixed_live) {
        unmap((void*)(uintptr_t)MIXED, PAGE);
        unmap((void*)(uintptr_t)(MIXED + PAGE), PAGE);
    }
    if (sparse_live) {
        unmap((void*)(uintptr_t)SPARSE, 2 * PAGE);
        unmap((void*)(uintptr_t)(SPARSE + 3 * PAGE), 2 * PAGE);
    }
    if (fd >= 0) close(fd);
    if (readonly >= 0) close(readonly);
    if (control >= 0) {
        command(control, DONE, 0, completed ? 0 : 1, 0);
        close(control);
    }
    printf("DEVICE_WB_USER %s assertions=%u failures=%u genuine_syscalls=1\n", completed && failures == 0 ? "PASS" : "FAIL", assertions,
           failures);
    fflush(stdout);
    return completed && failures == 0 ? 0 : 1;
}
