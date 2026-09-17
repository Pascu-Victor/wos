// Functional queue mapping and authority checks on a disposable four-CPU VM.
#include <callnums/net.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/process.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>

namespace {
struct Worker {
    unsigned long long pid{}, cpu{};
};
using Workers = std::array<Worker, 4>;
auto observe(const char* name, Workers& workers) -> bool {
    FILE* file = fopen("/proc/wki/netdiag", "r");
    if (!file) return false;
    char line[4096]{};
    unsigned seen = 0;
    while (fgets(line, sizeof(line), file)) {
        char actual[16]{};
        unsigned index = 0, pair = 0, active = 0, pairs = 0;
        if (sscanf(line, "virtio_net name=%15s ifindex=%u pair=%u active=%u pairs=%u", actual, &index, &pair, &active, &pairs) != 5 ||
            strcmp(actual, name))
            continue;
        char* worker = strstr(line, " worker_pid=");
        if (!worker || pair >= 4 || active != 1 || pairs != 4 || (seen & (1U << pair)) ||
            sscanf(worker, " worker_pid=%llu worker_cpu=%llu", &workers[pair].pid, &workers[pair].cpu) != 2 || workers[pair].pid == 0 ||
            workers[pair].cpu >= 4) {
            fclose(file);
            return false;
        }
        seen |= 1U << pair;
    }
    fclose(file);
    return seen == 15;
}
auto same(const Workers& a, const Workers& b) -> bool {
    for (unsigned i = 0; i < 4; ++i)
        if (a[i].pid != b[i].pid || a[i].cpu != b[i].cpu) return false;
    return true;
}
auto apply(const char* name, uint64_t mask) -> int64_t {
    struct Request {
        char name[16]{};
        uint64_t mask{};
    } request{};
    static_assert(sizeof(request) == 24);
    strcpy(request.name, name);
    request.mask = mask;
    return static_cast<int64_t>(syscall(ker::abi::callnums::net, static_cast<uint64_t>(ker::abi::net::ops::SET_DEV_CPU_AFFINITY),
                                        reinterpret_cast<uint64_t>(&request)));
}
auto profile(unsigned mode) -> bool {
    if (mode == 1 && (setgid(1000) || setuid(1000))) return false;
    if (mode == 2 && ker::process::prctl(0x574f5302, 0, 0, 0, 0)) return false;
    if (mode >= 3) {
        unsigned const ID = mode == 3 ? 101 : 102;
        if (ker::process::prctl(38, 1, 0, 0, 0) || ker::process::prctl(0x574f5303, mode == 3 ? 1 : 2, 0, 0, 0) || setgid(ID) || setuid(ID))
            return false;
        if (mode == 5 && ker::process::prctl(0x574f5302, 0, 0, 0, 0)) return false;
    }
    return true;
}
auto child_apply(const char* name, unsigned mode, uint64_t mask) -> int64_t {
    int pipefd[2]{};
    if (pipe(pipefd)) return -10000;
    pid_t const CHILD = fork();
    if (CHILD == 0) {
        close(pipefd[0]);
        int64_t const RESULT = profile(mode) ? apply(name, mask) : -10001;
        bool const SENT = write(pipefd[1], &RESULT, sizeof(RESULT)) == sizeof(RESULT);
        close(pipefd[1]);
        _exit(SENT ? 0 : 1);
    }
    close(pipefd[1]);
    int64_t result = -10002;
    ssize_t const READ = read(pipefd[0], &result, sizeof(result));
    close(pipefd[0]);
    int status = -1;
    if (CHILD < 0 || waitpid(CHILD, &status, 0) != CHILD || status || READ != sizeof(result)) return -10003;
    return result;
}
}  // namespace
auto main(int argc, char** argv) -> int {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc != 2 || strlen(argv[1]) >= 16 || geteuid() != 0) return 2;
    Workers original{}, before{}, after{};
    if (!observe(argv[1], original)) return 2;
    // Startup uses descending CPU order, which this ascending-mask ABI cannot
    // restore. Establish an explicit disposable-VM baseline and restore that
    // baseline between cases; the host removes this VM after the test.
    before = original;
    for (unsigned i = 0; i < 4; ++i) {
        printf("INITIAL pair=%u worker=%llu cpu=%llu\n", i, original[i].pid, original[i].cpu);
        original[i].cpu = i;
    }
    bool normalized = false;
    for (unsigned attempt = 0; attempt < 100; ++attempt) {
        int64_t const RESULT = apply(argv[1], 15);
        if (!observe(argv[1], after)) return 3;
        if (RESULT == 0) {
            normalized = same(original, after);
            break;
        }
        if (RESULT != -EBUSY || !same(before, after)) return 3;
        usleep(10000);
    }
    printf("BASELINE normalized=%d mask=15\n", normalized);
    if (!normalized) return 3;
    unsigned checks = 0, failures = 0, busy = 0;
    for (unsigned mode = 0; mode < 6; ++mode) {
        bool const ADMITTED = mode == 0 || mode == 4;
        for (unsigned operation = 0; operation < 4; ++operation) {
            uint64_t const MASK = operation == 0 ? 0 : operation == 1 ? 31 : operation == 2 ? ((1ULL << 63) | 2) : 12;
            int64_t const EXPECTED = !ADMITTED ? -EPERM : operation == 1 ? -EINVAL : operation == 2 ? -EBUSY : 0;
            if (!observe(argv[1], before)) return 3;
            bool passed = false;
            int64_t result = 0;
            for (unsigned attempt = 0; attempt < 64; ++attempt) {
                result = child_apply(argv[1], mode, MASK);
                if (!observe(argv[1], after)) return 3;
                if (result != 0 && !same(before, after)) break;
                if (result == -EBUSY && EXPECTED == 0) {
                    ++busy;
                    usleep(10000);
                    continue;
                }
                Workers wanted = before;
                if (result == 0 && operation == 3) {
                    wanted[0].cpu = 2;
                    wanted[1].cpu = 3;
                }
                passed = result == EXPECTED && same(wanted, after);
                break;
            }
            bool restored = same(original, after);
            for (unsigned attempt = 0; !restored && attempt < 100; ++attempt) {
                int64_t const RESTORE = apply(argv[1], 15);
                if (RESTORE != 0 && RESTORE != -EBUSY) break;
                if (!observe(argv[1], after)) break;
                restored = same(original, after);
                if (!restored) usleep(10000);
            }
            ++checks;
            if (!passed || !restored) ++failures;
            printf("CHECK mode=%u operation=%u result=%lld expected=%lld passed=%d restored=%d\n", mode, operation,
                   static_cast<long long>(result), static_cast<long long>(EXPECTED), passed, restored);
            if (!restored) return 3;
        }
    }
    printf("RESULT checks=%u failures=%u busy_retries=%u restored=1\n", checks, failures, busy);
    return failures ? 1 : 0;
}
