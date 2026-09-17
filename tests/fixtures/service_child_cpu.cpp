// Functional child-usage preservation across local and distributed exec.
#include <grp.h>
#include <signal.h>
#include <sys/times.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {
auto now() -> uint64_t {
    timespec value{};
    if (clock_gettime(CLOCK_MONOTONIC, &value)) _exit(90);
    return static_cast<uint64_t>(value.tv_sec) * 1000000 + value.tv_nsec / 1000;
}
auto reap_work() -> bool {
    pid_t const child = fork();
    if (!child) {
        auto const deadline = now() + 150000;
        uint64_t value = 1;
        while (now() < deadline) {
            for (unsigned i = 0; i < 10000; ++i) value = value * 1664525 + 1013904223;
            asm volatile("" : "+r"(value));
        }
        _exit(37);
    }
    if (child < 0) return false;
    int status{};
    pid_t result;
    do {
        result = waitpid(child, &status, 0);
    } while (result < 0 && errno == EINTR);
    return result == child && status == (37 << 8);
}
}  // namespace
int main(int argc, char** argv) {
    // stage mode uid source-path remote-path expected-user expected-system
    if (argc != 8) return 2;
    alarm(30);
    int const stage = atoi(argv[1]), mode = atoi(argv[2]);
    unsigned const uid = strtoul(argv[3], nullptr, 10);
    if (stage == 0 && uid && (setgroups(0, nullptr) || setgid(uid) || setuid(uid))) return 3;
    tms usage{};
    if (times(&usage) == static_cast<clock_t>(-1)) return 4;
    auto const expected_user = strtoll(argv[6], nullptr, 10), expected_system = strtoll(argv[7], nullptr, 10);
    std::array<char, 64> host{};
    if (gethostname(host.data(), host.size())) return 5;
    bool const imported = stage == 0 || (usage.tms_cutime == expected_user && usage.tms_cstime == expected_system);
    bool const identity = getuid() == uid && !strcmp(host.data(), mode && stage == 1 ? "wos-3" : "wos-0");
    if (!imported || !identity) {
        printf("FAIL stage=%d mode=%d uid=%u pid=%ld host=%s user=%lld system=%lld expected_user=%lld expected_system=%lld\n", stage, mode,
               uid, static_cast<long>(getpid()), host.data(), static_cast<long long>(usage.tms_cutime),
               static_cast<long long>(usage.tms_cstime), expected_user, expected_system);
        return 6;
    }
    if (stage < 2) {
        auto const before = usage.tms_cutime + usage.tms_cstime;
        if (!reap_work() || times(&usage) == static_cast<clock_t>(-1) || usage.tms_cutime + usage.tms_cstime <= before) return 7;
    }
    printf("PASS stage=%d mode=%d uid=%u pid=%ld host=%s user=%lld system=%lld\n", stage, mode, uid, static_cast<long>(getpid()),
           host.data(), static_cast<long long>(usage.tms_cutime), static_cast<long long>(usage.tms_cstime));
    fflush(stdout);
    if (stage == 2) return 0;
    std::array<char, 32> next{}, user{}, system{};
    snprintf(next.data(), next.size(), "%d", stage + 1);
    snprintf(user.data(), user.size(), "%lld", static_cast<long long>(usage.tms_cutime));
    snprintf(system.data(), system.size(), "%lld", static_cast<long long>(usage.tms_cstime));
    if (mode)
        execl("/usr/bin/on", "on", stage == 0 ? "wos-3" : "wos-0", stage == 0 ? argv[5] : argv[4], next.data(), argv[2], argv[3], argv[4],
              argv[5], user.data(), system.data(), nullptr);
    else
        execl(argv[4], argv[4], next.data(), argv[2], argv[3], argv[4], argv[5], user.data(), system.data(), nullptr);
    perror("exec child CPU fixture");
    return 8;
}
