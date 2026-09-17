#include <callnums/net.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
struct Observation {
    unsigned long long pid = 0, cpu = 0;
};
auto observe(const char* name, Observation& out) -> bool {
    FILE* file = fopen("/proc/wki/netdiag", "r");
    if (!file) return false;
    char line[4096]{};
    unsigned found = 0;
    while (fgets(line, sizeof(line), file)) {
        char actual[16]{};
        unsigned index = 0, pair = 0, active = 0, pairs = 0;
        if (sscanf(line, "virtio_net name=%15s ifindex=%u pair=%u active=%u pairs=%u", actual, &index, &pair, &active, &pairs) != 5 ||
            strcmp(actual, name))
            continue;
        char* worker = strstr(line, " worker_pid=");
        if (!worker || pair != 0 || active != 1 || pairs != 1 ||
            sscanf(worker, " worker_pid=%llu worker_cpu=%llu", &out.pid, &out.cpu) != 2 || out.pid == 0 || out.cpu >= 4) {
            fclose(file);
            return false;
        }
        ++found;
    }
    fclose(file);
    return found == 1;
}
auto apply(const char* name, uint64_t mask) -> int64_t {
    struct Request {
        char name[16]{};
        uint64_t mask{};
    } request{};
    static_assert(sizeof(request) == 24);
    memcpy(request.name, name, strlen(name) + 1);
    request.mask = mask;
    return static_cast<int64_t>(syscall(ker::abi::callnums::net, static_cast<uint64_t>(ker::abi::net::ops::SET_DEV_CPU_AFFINITY),
                                        reinterpret_cast<uint64_t>(&request)));
}
}  // namespace

auto main(int argc, char** argv) -> int {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc != 2 || strlen(argv[1]) >= 16 || geteuid() != 0) return 2;
    Observation before{}, after{};
    if (!observe(argv[1], before)) return 2;
    // The disposable test VM has four CPUs and exactly one queue on this NIC.
    unsigned const TARGET = before.cpu == 0 ? 1 : 0;
    uint64_t const MASK = (1ULL << TARGET) | (1ULL << (TARGET + 1));
    pid_t child = fork();
    if (child == 0) {
        if (setgid(1000) || setuid(1000)) _exit(2);
        int64_t const RESULT = apply(argv[1], MASK);
        printf("UNPRIVILEGED result=%lld\n", static_cast<long long>(RESULT));
        _exit(RESULT == -EPERM ? 0 : 1);
    }
    int status = -1;
    if (child < 0 || waitpid(child, &status, 0) != child || status != 0) return 2;
    bool const DENIAL_PRESERVED = observe(argv[1], after) && before.pid == after.pid && before.cpu == after.cpu;
    printf("UNPRIVILEGED_PARENT preserved=%d\n", DENIAL_PRESERVED);
    if (!DENIAL_PRESERVED) return 3;
    bool reached = false, unchanged = true;
    int64_t result = 0;
    for (unsigned attempt = 0; attempt < 32; ++attempt) {
        result = apply(argv[1], MASK);
        if (!observe(argv[1], after) || before.pid != after.pid) return 3;
        unchanged &= before.cpu == after.cpu;
        printf("ATTEMPT index=%u result=%lld worker=%llu before=%llu after=%llu unchanged=%d\n", attempt, static_cast<long long>(result),
               before.pid, before.cpu, after.cpu, unchanged);
        if (result == -EINVAL) {
            reached = true;
            break;
        }
        if (result != -EBUSY || !unchanged) break;
        usleep(10000);
    }
    bool restored = before.cpu == after.cpu;
    for (unsigned attempt = 0; !restored && attempt < 100; ++attempt) {
        int64_t const RESTORE = apply(argv[1], 1ULL << before.cpu);
        if (RESTORE != 0 && RESTORE != -EBUSY) break;
        if (!observe(argv[1], after) || after.pid != before.pid) break;
        restored = after.cpu == before.cpu;
        if (!restored) usleep(10000);
    }
    printf("RESULT passed=%d reached_einval=%d unchanged=%d restored=%d worker=%llu original=%llu final=%llu\n",
           reached && unchanged && restored, reached, unchanged, restored, before.pid, before.cpu, after.cpu);
    return reached && restored ? (unchanged ? 0 : 1) : 3;
}
