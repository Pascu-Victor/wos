// Exact declared service profiles, ordinary denied operations, owned targets only.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/process.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>

#include "../../modules/kern/src/abi/callnums/power.h"
#include "../../modules/kern/src/abi/ptrace.hpp"

namespace {
struct Profile {
    unsigned uid;
    unsigned grants;
    unsigned ceiling;
};
constexpr std::array PROFILES{Profile{100, 0, 0}, Profile{101, 1, 16384}, Profile{102, 2, 4096}, Profile{103, 0, 0}, Profile{104, 0, 0}};
auto control(unsigned option, unsigned value = 0) -> int64_t { return ker::process::prctl(option, value, 0, 0, 0); }
auto power(ker::abi::power::ops operation, uint64_t value = 0) -> int64_t {
    return static_cast<int64_t>(syscall(ker::abi::callnums::power, static_cast<uint64_t>(operation), value));
}
auto identity(const Profile& profile) -> bool {
    uid_t real = 0, effective = 0, saved = 0;
    gid_t greal = 0, geffective = 0, gsaved = 0;
    return !getresuid(&real, &effective, &saved) && !getresgid(&greal, &geffective, &gsaved) && real == profile.uid &&
           effective == profile.uid && saved == profile.uid && greal == profile.uid && geffective == profile.uid && gsaved == profile.uid &&
           getgroups(0, nullptr) == 0 && control(39) == 1 && control(0x574f5301) == profile.ceiling &&
           control(0x574f5304) == (profile.grants | 256U);
}
auto run_profile(const Profile& profile, pid_t target, const char* key) -> int {
    alarm(15);
    if (control(38, 1) || control(0x574f5303, profile.grants) || setgroups(0, nullptr) || setgid(profile.uid) || setuid(profile.uid) ||
        control(0x574f5302, profile.ceiling))
        return 2;
    unsigned checks = 0, failed = 0;
    auto check = [&](bool okay, const char* name) {
        ++checks;
        if (!okay) ++failed;
        printf("%s uid=%u %s\n", okay ? "PASS" : "FAIL", profile.uid, name);
    };
    check(identity(profile), "exact declared identity");
    int fd = open(key, O_RDONLY | O_CLOEXEC);
    check(fd == -1 && errno == EACCES, "root SSH directory private file denied");
    if (fd >= 0) close(fd);
    auto const trace = ker::process::ptrace(static_cast<uint64_t>(ker::abi::ptrace::request::ATTACH), target, 0, 0);
    check(trace == -EPERM, "unrelated owned root process ptrace denied");
    // Recovery on unexpected success touches only our owned sibling.
    if (trace == 0) static_cast<void>(ker::process::ptrace(static_cast<uint64_t>(ker::abi::ptrace::request::DETACH), target, 0, 0));
    auto const state = power(ker::abi::power::ops::GET_STATE);
    check(state == 0, "power idle before denial");
    check(power(ker::abi::power::ops::PREPARE) == -EPERM, "power preparation denied");
    check(power(ker::abi::power::ops::REBOOT, ker::abi::power::RB_DISABLE_CAD) == -EPERM, "power control denied");
    check(power(ker::abi::power::ops::GET_STATE) == state, "denied power state unchanged");
    check(static_cast<int64_t>(syscall(ker::abi::callnums::debug, 1)) == -EPERM, "undeclared DEBUG control denied");
    fd = open("/dev/journal", O_RDONLY | O_NONBLOCK | O_CLOEXEC);
    check(profile.grants == 1 ? fd >= 0 : fd == -1 && errno == EACCES, "declared journal read authority");
    if (fd >= 0) close(fd);
    fd = open("/proc/kperf", O_RDONLY | O_NONBLOCK | O_CLOEXEC);
    check(fd == -1 && errno == EACCES, "undeclared kernel diagnostics denied");
    if (fd >= 0) close(fd);
    check(seteuid(0) == -1 && errno == EPERM, "cannot restore root identity");
    check(control(0x574f5302, 65535) == -EPERM, "cannot restore discarded ceiling");
    check(control(0x574f5303, 3) == -EPERM, "cannot grow sealed grants");
    check(identity(profile), "denials preserve complete profile");
    printf("PROFILE uid=%u checks=%u failed=%u\n", profile.uid, checks, failed);
    fflush(stdout);
    return failed || checks != 14 ? 1 : 0;
}
}  // namespace

int main(int argc, char** argv) {
    char hostname[64]{};
    if (argc != 3 || geteuid() != 0 || gethostname(hostname, sizeof(hostname)) || strcmp(hostname, argv[2])) return 2;
    int key = open(argv[1], O_RDONLY | O_CLOEXEC);
    if (key < 0) return 3;
    close(key);
    if (power(ker::abi::power::ops::REBOOT, ker::abi::power::RB_DISABLE_CAD) != 0) return 4;
    int release[2];
    if (pipe(release)) return 5;
    pid_t target = fork();
    if (target == 0) {
        close(release[1]);
        alarm(90);
        char value = 0;
        int const result = read(release[0], &value, 1) == 1 ? 0 : 1;
        close(release[0]);
        _exit(result);
    }
    close(release[0]);
    if (target < 0) {
        close(release[1]);
        return 6;
    }
    unsigned failed = 0;
    for (const auto& profile : PROFILES) {
        pid_t child = fork();
        if (child == 0) {
            close(release[1]);
            _exit(run_profile(profile, target, argv[1]));
        }
        int status = -1;
        if (child < 0 || waitpid(child, &status, 0) != child || !WIFEXITED(status) || WEXITSTATUS(status) != 0) ++failed;
    }
    if (write(release[1], "X", 1) != 1) ++failed;
    close(release[1]);
    int status = -1;
    if (waitpid(target, &status, 0) != target || !WIFEXITED(status) || WEXITSTATUS(status)) ++failed;
    printf("%s service-role profiles=5 checks=70 failures=%u target-reaped=1 host=%s\n", failed ? "FAIL" : "PASS", failed, hostname);
    return failed ? 1 : 0;
}
