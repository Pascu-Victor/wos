#include "credentials.hpp"

#include <abi/callnums/net.h>
#include <abi/callnums/power.h>
#include <abi/callnums/process.h>
#include <abi/callnums/sys_log.h>
#include <abi/callnums/vfs.h>
#include <abi/callnums/vmem.h>
#include <dirent.h>
#include <fcntl.h>
#include <grp.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/shm.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/xattr.h>
#include <unistd.h>
#include <wos/netctl.h>

#include <abi/callnums.hpp>
#include <abi/ptrace.hpp>
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {
struct Checks {
    unsigned total{};
    unsigned failed{};
    void expect(bool condition, const char* name) {
        ++total;
        if (!condition) {
            ++failed;
            std::fprintf(stderr, "credentials: FAIL %s errno=%d\n", name, errno);
        }
    }
    auto finish() const -> int {
        std::printf("credentials: %u checks, %u failed\n", total, failed);
        return failed == 0 ? 0 : 1;
    }
};

auto user_ids(uid_t real, uid_t effective, uid_t saved) -> bool {
    uid_t r{}, e{}, s{};
    return getresuid(&r, &e, &s) == 0 && r == real && e == effective && s == saved;
}

auto group_ids(gid_t real, gid_t effective, gid_t saved) -> bool {
    gid_t r{}, e{}, s{};
    return getresgid(&r, &e, &s) == 0 && r == real && e == effective && s == saved;
}

auto wait_ok(pid_t child) -> bool {
    if (child < 0) return false;
    int status{};
    pid_t result;
    do {
        result = waitpid(child, &status, 0);
    } while (result < 0 && errno == EINTR);
    bool const OKAY = result == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
    if (!OKAY)
        std::fprintf(stderr, "credentials: child=%ld wait=%ld status=%#x errno=%d\n", static_cast<long>(child), static_cast<long>(result),
                     status, errno);
    return OKAY;
}

auto transitions() -> int {
    Checks checks;
    checks.expect(user_ids(0, 0, 0), "initial root IDs");
    checks.expect(setuid(static_cast<uid_t>(-1)) == -1 && errno == EINVAL && user_ids(0, 0, 0), "invalid UID leaves state unchanged");
    std::array<gid_t, 9> groups{4000, 4001, 4002, 4003, 4004, 4005, 4006, 4007, 4008};
    checks.expect(setgroups(8, groups.data()) == 0 && getgroups(0, nullptr) == 8, "bounded supplementary groups");
    checks.expect(setgroups(9, groups.data()) == -1 && errno == EINVAL && getgroups(0, nullptr) == 8,
                  "oversized groups leave state unchanged");
    std::array<gid_t, 8> output{};
    output.fill(123);
    checks.expect(getgroups(7, output.data()) == -1 && errno == EINVAL && output[0] == 123, "short group output is not modified");
    checks.expect(getgroups(8, output.data()) == 8 && output[0] == 4000 && output[7] == 4007, "group contents preserved");
    checks.expect(setgid(2000) == 0 && group_ids(2000, 2000, 2000), "permanent group drop");
    checks.expect(seteuid(1000) == 0 && user_ids(0, 1000, 0), "temporary user drop preserves saved identity");
    checks.expect(setgroups(0, nullptr) == -1 && errno == EPERM, "temporary nonroot cannot replace groups");
    checks.expect(seteuid(0) == 0 && user_ids(0, 0, 0), "saved identity restores temporary drop");
    checks.expect(setuid(1000) == 0 && user_ids(1000, 1000, 1000), "permanent user drop updates all IDs");
    checks.expect(seteuid(0) == -1 && errno == EPERM && user_ids(1000, 1000, 1000), "discarded root cannot be restored");
    checks.expect(setgroups(0, nullptr) == -1 && errno == EPERM && getgroups(0, nullptr) == 8, "denied group change is atomic");
    pid_t const CHILD = fork();
    if (CHILD == 0) _exit(user_ids(1000, 1000, 1000) && group_ids(2000, 2000, 2000) && getgroups(0, nullptr) == 8 ? 0 : 1);
    checks.expect(wait_ok(CHILD), "fork inherits complete dropped credentials");
    return checks.finish();
}

struct ThreadState {
    std::atomic<bool> start{};
    std::atomic<unsigned> ready{};
    std::atomic<unsigned> failed{};
    bool race{};
};

auto thread_observe(void* opaque) -> void* {
    auto& state = *static_cast<ThreadState*>(opaque);
    state.ready.fetch_add(1, std::memory_order_release);
    while (!state.start.load(std::memory_order_acquire)) sched_yield();
    if (state.race) {
        for (unsigned i = 0; i < 2000; ++i) {
            gid_t r{}, e{}, s{};
            if (getresgid(&r, &e, &s) != 0 || r != e || e != s || (r != 3000 && r != 3001)) {
                state.failed.fetch_add(1, std::memory_order_relaxed);
            }
        }
    } else {
        std::array<gid_t, 2> groups{};
        if (!user_ids(1000, 1000, 1000) || !group_ids(2000, 2000, 2000) || getgroups(2, groups.data()) != 2 || groups[0] != 5000 ||
            groups[1] != 5001 || seteuid(0) != -1 || errno != EPERM) {
            state.failed.fetch_add(1, std::memory_order_relaxed);
        }
    }
    return nullptr;
}

auto threads(bool race) -> int {
    Checks checks;
    ThreadState state;
    state.race = race;
    if (race && setgid(3000) != 0) return 1;
    std::array<pthread_t, 4> workers{};
    unsigned created = 0;
    for (auto& worker : workers) {
        int const ERROR = pthread_create(&worker, nullptr, thread_observe, &state);
        checks.expect(ERROR == 0, "create sibling thread");
        if (ERROR != 0) break;
        ++created;
    }
    while (state.ready.load(std::memory_order_acquire) != created) sched_yield();
    if (!race) {
        std::array<gid_t, 2> const GROUPS{5000, 5001};
        checks.expect(setgroups(GROUPS.size(), GROUPS.data()) == 0, "publish groups to siblings");
        checks.expect(setgid(2000) == 0 && setuid(1000) == 0, "drop entire thread group");
    }
    state.start.store(true, std::memory_order_release);
    if (race) {
        for (unsigned i = 0; i < 1000; ++i) {
            if (setgid(3000 + i % 2) != 0) state.failed.fetch_add(1, std::memory_order_relaxed);
            if (i % 16 == 0) sched_yield();
        }
    }
    for (unsigned i = 0; i < created; ++i) checks.expect(pthread_join(workers[i], nullptr) == 0, "join sibling thread");
    checks.expect(state.failed.load() == 0,
                  race ? "GETRESGID never observes a torn snapshot" : "siblings observe drop and cannot regain root");
    return checks.finish();
}

auto control_call(ker::abi::callnums subsystem, uint64_t operation, uint64_t argument = 0) -> int64_t {
    uint64_t result = static_cast<uint64_t>(subsystem);
    asm volatile("syscall" : "+a"(result) : "D"(operation), "S"(argument) : "rcx", "r11", "memory");
    return static_cast<int64_t>(result);
}

auto controls() -> int {
    Checks checks;
    std::array<char, 256> before{};
    std::array<char, 256> after{};
    checks.expect(gethostname(before.data(), before.size()) == 0, "read hostname before denial");
    auto const POWER = ker::abi::callnums::POWER;
    auto const REBOOT = static_cast<uint64_t>(ker::abi::power::ops::REBOOT);
    auto const STATE = static_cast<uint64_t>(ker::abi::power::ops::GET_STATE);
    auto const PREPARE = static_cast<uint64_t>(ker::abi::power::ops::PREPARE);
    checks.expect(control_call(POWER, REBOOT, ker::abi::power::RB_DISABLE_CAD) == 0, "root power control compatibility");
    checks.expect(sethostname(before.data(), std::strlen(before.data())) == 0, "root hostname control compatibility");
    checks.expect(setuid(1000) == 0, "drop host control authority");
    checks.expect(sethostname("forbidden", 9) == -1 && errno == EPERM, "hostname change denied");
    checks.expect(gethostname(after.data(), after.size()) == 0 && before == after, "denied hostname unchanged");
    checks.expect(control_call(ker::abi::callnums::DEBUG, 0) == -EPERM, "disable interrupts denied");
    checks.expect(control_call(ker::abi::callnums::DEBUG, 1) == -EPERM, "enable interrupts denied");
    int64_t const BEFORE = control_call(POWER, STATE);
    checks.expect(BEFORE == 0, "power starts idle");
    checks.expect(control_call(POWER, PREPARE) == -EPERM, "power preparation denied");
    checks.expect(control_call(POWER, REBOOT, ker::abi::power::RB_POWER_OFF) == -EPERM, "poweroff denied");
    checks.expect(control_call(POWER, REBOOT, ker::abi::power::RB_DISABLE_CAD) == -EPERM, "power control no-op denied");
    checks.expect(control_call(POWER, STATE) == BEFORE, "denied power state unchanged");
    return checks.finish();
}

volatile sig_atomic_t observed_signals{};
void observe_signal(int) { observed_signals = observed_signals + 1; }

auto signal_policy() -> int {
    Checks checks;
    std::array<int, 2> ready{};
    std::array<int, 2> release{};
    if (pipe(ready.data()) != 0 || pipe(release.data()) != 0) return 1;
    pid_t const VICTIM = fork();
    if (VICTIM == 0) {
        close(ready[0]);
        close(release[1]);
        observed_signals = 0;
        if (signal(SIGUSR1, observe_signal) == SIG_ERR || setpgid(0, 0) != 0) _exit(1);
        char value = 'R';
        if (write(ready[1], &value, 1) != 1) _exit(1);
        ssize_t result;
        do {
            result = read(release[0], &value, 1);
        } while (result < 0 && errno == EINTR);
        _exit(result == 1 && observed_signals == 0 ? 0 : 1);
    }
    close(ready[1]);
    close(release[0]);
    if (VICTIM < 0) return 1;
    char value{};
    checks.expect(read(ready[0], &value, 1) == 1 && value == 'R', "root victim ready in separate process group");
    checks.expect(kill(VICTIM, 0) == 0, "root signal permission compatibility");
    pid_t const ATTACKER = fork();
    if (ATTACKER == 0) {
        Checks child;
        child.expect(setuid(1000) == 0, "drop signal authority");
        child.expect(kill(VICTIM, 0) == -1 && errno == EPERM, "signal zero checks ownership");
        child.expect(kill(VICTIM, SIGUSR1) == -1 && errno == EPERM, "foreign signal denied");
        child.expect(kill(-VICTIM, 0) == -1 && errno == EPERM, "group signal zero checks ownership");
        child.expect(kill(-VICTIM, SIGUSR1) == -1 && errno == EPERM, "foreign process group signal denied");
        child.expect(kill(VICTIM, SIGCONT) == 0, "SIGCONT same-session exception");
        child.expect(kill(getpid(), 0) == 0 && kill(0, 0) == 0, "own and mixed group signal zero allowed");
        observed_signals = 0;
        child.expect(signal(SIGUSR1, observe_signal) != SIG_ERR && kill(getpid(), SIGUSR1) == 0 && observed_signals == 1,
                     "same-user signal delivery");
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(ATTACKER), "signal denial subprocess");
    value = 'X';
    checks.expect(write(release[1], &value, 1) == 1, "release root victim");
    checks.expect(wait_ok(VICTIM), "denied signals did not reach victim");
    close(ready[0]);
    close(release[1]);
    return checks.finish();
}

struct PriorityRace {
    pid_t target{};
    std::atomic<bool> start{};
    std::atomic<unsigned> failed{};
};

auto priority_racer(void* opaque) -> void* {
    auto& state = *static_cast<PriorityRace*>(opaque);
    while (!state.start.load(std::memory_order_acquire)) sched_yield();
    int previous = -20;
    for (unsigned i = 0; i < 200; ++i) {
        int const REQUEST = static_cast<int>((i * 7) % 20);
        int const RESULT = setpriority(PRIO_PROCESS, state.target, REQUEST);
        if (RESULT != 0 && errno != EACCES) state.failed.fetch_add(1);
        errno = 0;
        int const CURRENT = getpriority(PRIO_PROCESS, state.target);
        if (errno != 0 || CURRENT < previous) state.failed.fetch_add(1);
        previous = CURRENT;
    }
    return nullptr;
}

auto priority_policy() -> int {
    Checks checks;
    checks.expect(setpriority(PRIO_PROCESS, 0, 0) == 0, "root resets priority");
    std::array<int, 2> ready{};
    std::array<int, 2> release{};
    if (pipe(ready.data()) != 0 || pipe(release.data()) != 0) return 1;
    std::array<pid_t, 2> victims{};
    for (unsigned i = 0; i < victims.size(); ++i) {
        victims[i] = fork();
        if (victims[i] == 0) {
            close(ready[0]);
            close(release[1]);
            if (setuid(1000 + i) != 0) _exit(1);
            char value = 'R';
            if (write(ready[1], &value, 1) != 1) _exit(1);
            ssize_t result;
            do {
                result = read(release[0], &value, 1);
            } while (result < 0 && errno == EINTR);
            _exit(result == 1 ? 0 : 1);
        }
        if (victims[i] < 0) return 1;
    }
    close(ready[1]);
    close(release[0]);
    for (unsigned i = 0; i < victims.size(); ++i) {
        char value{};
        checks.expect(read(ready[0], &value, 1) == 1 && value == 'R', "priority target ready");
    }
    checks.expect(setuid(1000) == 0, "drop priority authority");
    checks.expect(setpriority(PRIO_PROCESS, 0, -1) == -1 && errno == EACCES && getpriority(PRIO_PROCESS, 0) == 0,
                  "unprivileged self raise denied without change");
    checks.expect(setpriority(PRIO_PROCESS, victims[0], 10) == 0 && getpriority(PRIO_PROCESS, victims[0]) == 10,
                  "same-user process priority may be lowered");
    checks.expect(setpriority(PRIO_PROCESS, victims[0], 0) == -1 && errno == EACCES && getpriority(PRIO_PROCESS, victims[0]) == 10,
                  "same-user process cannot regain discarded priority");
    checks.expect(setpriority(PRIO_PROCESS, victims[1], 19) == -1 && errno == EPERM && getpriority(PRIO_PROCESS, victims[1]) == 0,
                  "foreign process priority unchanged on denial");
    checks.expect(setpriority(PRIO_PROCESS, victims[0], 100) == 0 && getpriority(PRIO_PROCESS, victims[0]) == 19,
                  "priority retains clamping behavior");
    PriorityRace state;
    state.target = getpid();
    std::array<pthread_t, 4> workers{};
    unsigned created = 0;
    for (auto& worker : workers) {
        if (pthread_create(&worker, nullptr, priority_racer, &state) != 0) break;
        ++created;
    }
    checks.expect(created == workers.size(), "create priority race workers");
    state.start.store(true, std::memory_order_release);
    for (unsigned i = 0; i < created; ++i) checks.expect(pthread_join(workers[i], nullptr) == 0, "join priority worker");
    checks.expect(state.failed.load() == 0 && getpriority(PRIO_PROCESS, 0) == 19, "racing updates cannot regain priority");
    char const VALUES[] = "XX";
    checks.expect(write(release[1], VALUES, 2) == 2, "release priority targets");
    for (pid_t victim : victims) checks.expect(wait_ok(victim), "priority target exits cleanly");
    close(ready[0]);
    close(release[1]);
    return checks.finish();
}

auto process_call(ker::abi::process::procmgmt_ops operation, uint64_t a2, uint64_t a3 = 0, uint64_t a4 = 0, uint64_t a5 = 0,
                  uint64_t a6 = 0) -> int64_t {
    uint64_t result = static_cast<uint64_t>(ker::abi::callnums::PROCESS);
    register uint64_t arg4 asm("r8") = a4;
    register uint64_t arg5 asm("r9") = a5;
    register uint64_t arg6 asm("r10") = a6;
    asm volatile("syscall"
                 : "+a"(result)
                 : "D"(static_cast<uint64_t>(operation)), "S"(a2), "d"(a3), "r"(arg4), "r"(arg5), "r"(arg6)
                 : "rcx", "r11", "memory");
    return static_cast<int64_t>(result);
}

using CredentialControl = ker::abi::process::CredentialControl;
auto restriction_call(CredentialControl option, uint64_t value = 0, uint64_t spare1 = 0, uint64_t spare2 = 0, uint64_t spare3 = 0)
    -> int64_t {
    return process_call(ker::abi::process::procmgmt_ops::PRCTL, static_cast<uint64_t>(option), value, spare1, spare2, spare3);
}

auto network_call(ker::abi::net::ops operation, uint64_t arg1 = 0, uint64_t arg2 = 0) -> int64_t {
    uint64_t result = static_cast<uint64_t>(ker::abi::callnums::NET);
    register uint64_t arg3 asm("r8") = 0;
    register uint64_t arg4 asm("r9") = 0;
    register uint64_t arg5 asm("r10") = 0;
    asm volatile("syscall"
                 : "+a"(result)
                 : "D"(static_cast<uint64_t>(operation)), "S"(arg1), "d"(arg2), "r"(arg3), "r"(arg4), "r"(arg5)
                 : "rcx", "r11", "memory");
    return static_cast<int64_t>(result);
}

auto service_privilege_operations(uint64_t mask, uid_t uid = 1000) -> int {
    using ker::abi::sys_log::JournalRecord;
    Checks checks;
    auto const CEILING = ker::abi::process::service_privilege_ceiling(mask);
    checks.expect(user_ids(uid, uid, uid) && group_ids(uid, uid, uid) && getgroups(0, nullptr) == 0,
                  "delegated service retains permanent nonroot identity");
    checks.expect(restriction_call(CredentialControl::GET_SERVICE_PRIVILEGES) ==
                          static_cast<int64_t>(mask | ker::abi::process::SERVICE_PRIVILEGES_LOCKED) &&
                      restriction_call(CredentialControl::GET_NO_NEW_PRIVILEGES) == 1 &&
                      restriction_call(CredentialControl::GET_AUTHORITY_CEILING) == static_cast<int64_t>(CEILING),
                  "delegation mask, seal, NNP and exact ceiling survive exec");
    checks.expect(seteuid(0) == -1 && errno == EPERM && setuid(0) == -1 && errno == EPERM, "delegation cannot regain root IDs");
    checks.expect(kill(1, 0) == -1 && errno == EPERM, "delegation cannot signal init");
    checks.expect(mount("none", "/tmp", "tmpfs", 0, nullptr) == -1 && errno == EPERM, "delegation cannot mount");
    for (const char* path : {"/proc/kperf", "/proc/kperfctl", "/etc/dropbear/dropbear_rsa_host_key"}) {
        int const FD = open(path, O_RDONLY | O_NONBLOCK | O_CLOEXEC);
        checks.expect(FD == -1 && (errno == EACCES || errno == EPERM), "delegation excludes general diagnostics and host key");
        if (FD >= 0) close(FD);
    }
    int const JOURNAL = open("/dev/journal", O_RDONLY | O_NONBLOCK | O_CLOEXEC);
    if ((mask & 1) != 0) {
        JournalRecord record{};
        checks.expect(JOURNAL >= 0 && read(JOURNAL, &record, sizeof(record)) == sizeof(record) &&
                          record.magic == ker::abi::sys_log::JOURNAL_RECORD_MAGIC,
                      "nonroot journal privilege reads valid records");
    } else {
        checks.expect(JOURNAL == -1 && errno == EACCES, "network privilege cannot read journal");
    }
    for (int family : {AF_INET, AF_INET6}) {
        int const RAW = socket(family, SOCK_RAW | SOCK_CLOEXEC, family == AF_INET ? IPPROTO_ICMP : IPPROTO_ICMPV6);
        checks.expect((mask & 2) != 0 ? RAW >= 0 : RAW == -1 && errno == EPERM, "raw socket creation follows only network delegation");
        if (RAW >= 0) close(RAW);
    }
    std::array<wos_net_if_info, 16> interfaces{};
    size_t count = interfaces.size();
    checks.expect(wos_net_if_list(interfaces.data(), &count) == 0 && count <= interfaces.size(),
                  "delegated service retains public network queries");
    wos_net_link_set_req link{};
    for (size_t i = 0; i < std::min(count, interfaces.size()); ++i)
        if (std::strcmp(interfaces[i].name, "lo") == 0) {
            link.ifindex = interfaces[i].ifindex;
            link.fields = WOS_NET_LINK_SET_MTU;
            link.mtu = interfaces[i].mtu;
        }
    int const LINK_RESULT = wos_net_link_set(&link);
    checks.expect(link.ifindex != 0 && ((mask & 2) != 0 ? LINK_RESULT == 0 : LINK_RESULT == -1 && errno == EPERM),
                  "same-value loopback MTU update follows network privilege");
    checks.expect(restriction_call(CredentialControl::SET_SERVICE_PRIVILEGES, 0) == 0 &&
                      restriction_call(CredentialControl::SET_SERVICE_PRIVILEGES, mask) == -EPERM,
                  "nonroot delegated service can drop but never regain its grant");
    if (JOURNAL >= 0) {
        std::array<unsigned char, sizeof(JournalRecord)> bytes{};
        bytes.fill(0xA5);
        checks.expect(read(JOURNAL, bytes.data(), bytes.size()) == -1 && errno == EACCES &&
                          std::all_of(bytes.begin(), bytes.end(), [](unsigned char c) { return c == 0xA5; }),
                      "dropping journal grant revokes retained descriptor without output");
        struct pollfd poller{.fd = JOURNAL, .events = POLLIN, .revents = 0};
        checks.expect(poll(&poller, 1, 0) == 1 && (poller.revents & POLLERR) != 0 && (poller.revents & POLLIN) == 0,
                      "dropped journal grant removes read readiness");
        close(JOURNAL);
    }
    int const RAW = socket(AF_INET, SOCK_RAW, IPPROTO_ICMP);
    checks.expect(RAW == -1 && errno == EPERM, "dropped network grant denies new raw socket");
    if (RAW >= 0) close(RAW);
    checks.expect(wos_net_link_set(&link) == -1 && errno == EPERM, "dropped grant denies network mutation");
    checks.expect(restriction_call(CredentialControl::GET_SERVICE_PRIVILEGES) == ker::abi::process::SERVICE_PRIVILEGES_LOCKED &&
                      restriction_call(CredentialControl::GET_AUTHORITY_CEILING) == static_cast<int64_t>(CEILING),
                  "grant drop preserves independent ceiling and seal");
    return checks.finish();
}

auto service_privilege_policy(uint64_t mask) -> int {
    Checks checks;
    checks.expect(restriction_call(CredentialControl::GET_SERVICE_PRIVILEGES) == 0, "initial root has no explicit service grant");
    checks.expect(restriction_call(CredentialControl::SET_SERVICE_PRIVILEGES, mask) == -EPERM, "grant requires NNP");
    checks.expect(restriction_call(CredentialControl::SET_SERVICE_PRIVILEGES, 4) == -EINVAL, "grant rejects unknown bits");
    for (auto option : {CredentialControl::GET_SERVICE_PRIVILEGES, CredentialControl::SET_SERVICE_PRIVILEGES}) {
        checks.expect(restriction_call(option, 0, 1) == -EINVAL, "service control rejects spare arg3");
        checks.expect(restriction_call(option, 0, 0, 1) == -EINVAL, "service control rejects spare arg4");
        checks.expect(restriction_call(option, 0, 0, 0, 1) == -EINVAL, "service control rejects spare arg5");
    }
    checks.expect(restriction_call(CredentialControl::GET_SERVICE_PRIVILEGES, 1) == -EINVAL &&
                      restriction_call(CredentialControl::GET_SERVICE_PRIVILEGES) == 0,
                  "rejected controls preserve unsealed state");
    checks.expect(restriction_call(CredentialControl::SET_NO_NEW_PRIVILEGES, 1) == 0 &&
                      restriction_call(CredentialControl::SET_SERVICE_PRIVILEGES, mask) == 0,
                  "trusted root installs and seals service privilege");
    checks.expect(process_call(ker::abi::process::procmgmt_ops::PRCTL, 3) == 0, "grant publication resets dumpability");
    checks.expect(restriction_call(CredentialControl::SET_SERVICE_PRIVILEGES, mask) == 0, "idempotent sealed grant succeeds");
    if (mask != 3)
        checks.expect(restriction_call(CredentialControl::SET_SERVICE_PRIVILEGES, 3) == -EPERM,
                      "even trusted root cannot grow sealed grant");
    checks.expect(setgroups(0, nullptr) == 0 && setgid(1000) == 0 && setuid(1000) == 0, "trusted setup permanently drops service IDs");
    checks.expect(restriction_call(CredentialControl::DROP_AUTHORITY_CEILING, ker::abi::process::service_privilege_ceiling(mask)) == 0,
                  "service retains only matching ceiling");
    pid_t const CHILD = fork();
    if (CHILD == 0) {
        char const* text = mask == 1 ? "1" : (mask == 2 ? "2" : "3");
        execl("/usr/bin/testprog", "testprog", "credentials", "delegation-check", text, nullptr);
        _exit(127);
    }
    checks.expect(wait_ok(CHILD), "fork and exec preserve delegated operation boundary");
    checks.expect(restriction_call(CredentialControl::GET_SERVICE_PRIVILEGES) ==
                      static_cast<int64_t>(mask | ker::abi::process::SERVICE_PRIVILEGES_LOCKED),
                  "child grant drop leaves parent unchanged");
    checks.expect(restriction_call(CredentialControl::DROP_AUTHORITY_CEILING, 0) == 0, "service can discard matching ceiling");
    int const JOURNAL = open("/dev/journal", O_RDONLY | O_NONBLOCK);
    checks.expect(JOURNAL == -1 && errno == EACCES, "ceiling drop disables journal delegation");
    if (JOURNAL >= 0) close(JOURNAL);
    int const RAW = socket(AF_INET, SOCK_RAW, IPPROTO_ICMP);
    checks.expect(RAW == -1 && errno == EPERM, "ceiling drop disables network delegation");
    if (RAW >= 0) close(RAW);
    checks.expect(
        restriction_call(CredentialControl::SET_SERVICE_PRIVILEGES, mask) == 0 &&
            restriction_call(CredentialControl::DROP_AUTHORITY_CEILING, ker::abi::process::service_privilege_ceiling(mask)) == -EPERM,
        "retained sealed grant cannot restore removed ceiling");
    return checks.finish();
}

auto network_administration_policy(bool root_without_authority) -> int {
    using Op = ker::abi::net::ops;
    Checks checks;
    std::array<wos_net_if_info, 16> interfaces{};
    size_t count = interfaces.size();
    checks.expect(wos_net_if_list(interfaces.data(), &count) == 0 && count <= interfaces.size(), "root lists interfaces");
    wos_net_if_info before{};
    for (size_t i = 0; i < std::min(count, interfaces.size()); ++i)
        if (std::strcmp(interfaces[i].name, "lo") == 0) before = interfaces[i];
    if (before.ifindex == 0) {
        checks.expect(false, "loopback fixture exists");
        return checks.finish();
    }
    wos_net_link_set_req link{};
    link.ifindex = before.ifindex;
    link.fields = WOS_NET_LINK_SET_MTU;
    link.mtu = before.mtu;
    checks.expect(wos_net_link_set(&link) == 0, "root can retain loopback MTU through network control");
    for (auto family : {AF_INET, AF_INET6}) {
        int const RAW = socket(family, SOCK_RAW | SOCK_CLOEXEC, family == AF_INET ? IPPROTO_ICMP : IPPROTO_ICMPV6);
        checks.expect(RAW >= 0, "root creates raw socket");
        if (RAW >= 0) close(RAW);
    }
    if (root_without_authority) {
        constexpr uint64_t BIT = uint64_t{1} << static_cast<unsigned>(ker::abi::process::CredentialAuthority::NETWORK_ADMIN);
        checks.expect(restriction_call(CredentialControl::DROP_AUTHORITY_CEILING, ker::abi::process::CREDENTIAL_AUTHORITY_ALL & ~BIT) == 0,
                      "drop only network authority while remaining root");
    } else {
        checks.expect(setuid(1000) == 0, "drop user for network denials");
    }
    int const BASE_FD = socket(AF_INET, SOCK_DGRAM, 0);
    checks.expect(BASE_FD >= 0, "ordinary UDP socket remains public");
    if (BASE_FD >= 0) close(BASE_FD);
    for (int attempt = 0; attempt < 8; ++attempt) {
        for (auto family : {AF_INET, AF_INET6}) {
            int const RAW = socket(family, SOCK_RAW | SOCK_CLOEXEC | SOCK_NONBLOCK, family == AF_INET ? IPPROTO_ICMP : IPPROTO_ICMPV6);
            checks.expect(RAW == -1 && errno == EPERM, "raw socket creation requires retained network authority");
            if (RAW >= 0) close(RAW);
        }
    }
    int const AFTER_FD = socket(AF_INET, SOCK_DGRAM, 0);
    checks.expect(AFTER_FD == BASE_FD, "raw denials reserve no descriptors");
    if (AFTER_FD >= 0) close(AFTER_FD);
    int const TCP = socket(AF_INET, SOCK_STREAM, 0);
    checks.expect(TCP >= 0, "ordinary TCP socket remains public");
    if (TCP >= 0) close(TCP);
    checks.expect(socket(AF_UNSPEC, SOCK_RAW, IPPROTO_ICMP) == -1 && errno == EAFNOSUPPORT, "unsupported family keeps errno");
    checks.expect(socket(AF_INET, SOCK_RAW, 0) == -1 && errno == EPROTONOSUPPORT, "unsupported raw protocol keeps errno");
    std::array<uint8_t, 48> request{};
    std::memcpy(request.data(), "lo", 3);
    uint32_t const NEW_MTU = before.mtu + 1;
    std::memcpy(request.data() + 16, &NEW_MTU, sizeof(NEW_MTU));
    auto const ORIGINAL = request;
    for (uint32_t ioctl : {0x8914U, 0x8916U, 0x891CU, 0x8922U, 0x8923U, 0x8924U, 0x8937U, 0x8943U, 0x890BU, 0x890CU}) {
        checks.expect(network_call(Op::IOCTL_NET, ioctl, reinterpret_cast<uint64_t>(request.data())) == -EPERM,
                      "legacy network mutation denied");
        checks.expect(request == ORIGINAL, "denied legacy ioctl leaves caller buffer unchanged");
    }
    for (uint32_t ioctl : {0x8913U, 0x8915U, 0x891BU, 0x8927U, 0x8921U, 0x8933U, 0x8942U}) {
        request.fill(0);
        std::memcpy(request.data(), "lo", 3);
        checks.expect(network_call(Op::IOCTL_NET, ioctl, reinterpret_cast<uint64_t>(request.data())) == 0,
                      "legacy network query remains public");
    }
    checks.expect(network_call(Op::IOCTL_NET, 0x8922, 1) == -EFAULT, "legacy request staging preserves EFAULT");
    std::array<uint8_t, 128> structured{};
    std::memcpy(structured.data(), &before.ifindex, sizeof(before.ifindex));
    auto const STRUCTURED_BEFORE = structured;
    for (auto operation : {Op::NETCTL_ADDR_SET, Op::NETCTL_ADDR_DEL, Op::NETCTL_ADDR_SET_V2, Op::NETCTL_ROUTE_SET, Op::NETCTL_ROUTE_DEL,
                           Op::NETCTL_LINK_SET, Op::SET_DEV_CPU_AFFINITY}) {
        checks.expect(network_call(operation, reinterpret_cast<uint64_t>(structured.data())) == -EPERM,
                      "versioned network and queue-affinity mutation denied");
        checks.expect(structured == STRUCTURED_BEFORE, "denied versioned control leaves caller buffer unchanged");
        checks.expect(network_call(operation, 1) == -EFAULT, "versioned request staging preserves EFAULT");
    }
    link.mtu = NEW_MTU;
    checks.expect(wos_net_link_set(&link) == -1 && errno == EPERM, "well-formed MTU change denied through libc");
    std::array<wos_net_addr_info, 256> addresses{};
    count = addresses.size();
    checks.expect(wos_net_addr_list(addresses.data(), &count) == 0, "address enumeration remains public");
    std::array<wos_net_route_record, 128> routes{};
    count = routes.size();
    checks.expect(wos_net_route_list(routes.data(), &count) == 0, "route enumeration remains public");
    count = interfaces.size();
    checks.expect(wos_net_if_list(interfaces.data(), &count) == 0 && count <= interfaces.size(), "interface enumeration remains public");
    wos_net_if_info after{};
    for (size_t i = 0; i < std::min(count, interfaces.size()); ++i)
        if (interfaces[i].ifindex == before.ifindex) after = interfaces[i];
    checks.expect(std::memcmp(&before, &after, sizeof(before)) == 0, "denied network operations leave interface state unchanged");
    return checks.finish();
}

auto privilege_restrictions() -> int {
    Checks checks;
    constexpr uint64_t ALL = ker::abi::process::CREDENTIAL_AUTHORITY_ALL;
    constexpr uint64_t MOUNT = uint64_t{1} << static_cast<unsigned>(ker::abi::process::CredentialAuthority::MOUNT);
    checks.expect(restriction_call(CredentialControl::GET_NO_NEW_PRIVILEGES) == 0 &&
                      restriction_call(CredentialControl::GET_AUTHORITY_CEILING) == ALL,
                  "initial process restriction state");
    checks.expect(restriction_call(CredentialControl::SET_NO_NEW_PRIVILEGES, 0) == -EINVAL &&
                      restriction_call(CredentialControl::SET_NO_NEW_PRIVILEGES, 2) == -EINVAL,
                  "no-new-privileges accepts only irreversible set-one");
    for (unsigned slot = 0; slot < 4; ++slot) {
        std::array<uint64_t, 4> args{};
        args[slot] = 1;
        checks.expect(restriction_call(CredentialControl::GET_NO_NEW_PRIVILEGES, args[0], args[1], args[2], args[3]) == -EINVAL &&
                          restriction_call(CredentialControl::GET_AUTHORITY_CEILING, args[0], args[1], args[2], args[3]) == -EINVAL,
                      "restriction queries reject unused arguments");
        if (slot != 0) {
            checks.expect(restriction_call(CredentialControl::SET_NO_NEW_PRIVILEGES, 1, args[1], args[2], args[3]) == -EINVAL &&
                              restriction_call(CredentialControl::DROP_AUTHORITY_CEILING, ALL, args[1], args[2], args[3]) == -EINVAL,
                          "restriction updates reject unused arguments");
        }
    }
    checks.expect(restriction_call(CredentialControl::DROP_AUTHORITY_CEILING, uint64_t{1} << 63) == -EINVAL &&
                      restriction_call(CredentialControl::GET_NO_NEW_PRIVILEGES) == 0 &&
                      restriction_call(CredentialControl::GET_AUTHORITY_CEILING) == ALL,
                  "invalid restrictions leave the entire policy unchanged");
    checks.expect(restriction_call(CredentialControl::SET_NO_NEW_PRIVILEGES, 1) == 0 &&
                      restriction_call(CredentialControl::SET_NO_NEW_PRIVILEGES, 1) == 0 &&
                      restriction_call(CredentialControl::GET_NO_NEW_PRIVILEGES) == 1,
                  "no-new-privileges is idempotent and observable");
    checks.expect(restriction_call(CredentialControl::SET_NO_NEW_PRIVILEGES, 0) == -EINVAL &&
                      restriction_call(CredentialControl::GET_NO_NEW_PRIVILEGES) == 1,
                  "no-new-privileges cannot be cleared");
    checks.expect(seteuid(1000) == 0 && seteuid(0) == 0 && user_ids(0, 0, 0), "no-new-privileges preserves saved-ID syscall semantics");
    auto restrict_sibling = [](void*) -> void* {
        uint64_t const CEILING = ker::abi::process::CREDENTIAL_AUTHORITY_ALL &
                                 ~(uint64_t{1} << static_cast<unsigned>(ker::abi::process::CredentialAuthority::MOUNT));
        return reinterpret_cast<void*>(static_cast<intptr_t>(restriction_call(CredentialControl::DROP_AUTHORITY_CEILING, CEILING)));
    };
    pthread_t sibling{};
    int const CREATED = pthread_create(&sibling, nullptr, restrict_sibling, nullptr);
    void* result = reinterpret_cast<void*>(1);
    checks.expect(CREATED == 0 && pthread_join(sibling, &result) == 0 && result == nullptr,
                  "sibling can reduce the shared process ceiling");
    checks.expect(restriction_call(CredentialControl::GET_AUTHORITY_CEILING) == (ALL & ~MOUNT) && geteuid() == 0,
                  "root identity does not restore a dropped authority");
    checks.expect(mount("none", "/tmp", "tmpfs", 0, nullptr) == -1 && errno == EPERM, "root without MOUNT is denied before namespace work");
    checks.expect(restriction_call(CredentialControl::DROP_AUTHORITY_CEILING, ALL) == -EPERM &&
                      restriction_call(CredentialControl::GET_AUTHORITY_CEILING) == (ALL & ~MOUNT),
                  "dropped authority cannot be restored and denial leaves ceiling unchanged");
    pid_t const USER = fork();
    if (USER == 0) {
        if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        bool const OK = restriction_call(CredentialControl::DROP_AUTHORITY_CEILING, 0) == 0 &&
                        restriction_call(CredentialControl::GET_AUTHORITY_CEILING) == 0 &&
                        restriction_call(CredentialControl::GET_NO_NEW_PRIVILEGES) == 1 &&
                        restriction_call(CredentialControl::DROP_AUTHORITY_CEILING, ALL) == -EPERM && setuid(0) == -1 && errno == EPERM;
        _exit(OK ? 0 : 1);
    }
    checks.expect(wait_ok(USER), "unprivileged fork child can only reduce inherited restrictions");
    checks.expect(restriction_call(CredentialControl::GET_AUTHORITY_CEILING) == (ALL & ~MOUNT),
                  "child restriction does not mutate parent policy");
    checks.expect(restriction_call(CredentialControl::DROP_AUTHORITY_CEILING, 0) == 0 &&
                      restriction_call(CredentialControl::DROP_AUTHORITY_CEILING, 0) == 0,
                  "root can idempotently discard all authority");
    pid_t const EXEC = fork();
    if (EXEC == 0) {
        execl("/usr/bin/testprog", "testprog", "credentials", "restriction-check", static_cast<char*>(nullptr));
        _exit(1);
    }
    checks.expect(wait_ok(EXEC), "ordinary exec inherits irreversible restrictions");
    pid_t const REMOTE = fork();
    if (REMOTE == 0) {
        constexpr const char* PEER = "goal19-missing-peer";
        std::array<char, 256> hostname{};
        if (gethostname(hostname.data(), hostname.size()) != 0) _exit(1);
        std::array<gid_t, 8> before_groups{}, after_groups{};
        int const GROUP_COUNT = getgroups(before_groups.size(), before_groups.data());
        if (GROUP_COUNT < 0) _exit(1);
        // WOS WKI_TARGET_FLAG_STRICT is bit zero in the public process ABI.
        if (process_call(ker::abi::process::procmgmt_ops::SETWKITARGET, reinterpret_cast<uint64_t>(PEER), std::strlen(PEER), 1U) != 0)
            _exit(1);
        execl("/usr/bin/testprog", "testprog", "credentials", "strict-restricted-exec-must-fail", static_cast<char*>(nullptr));
        // Peer lookup precedes credential wire-capability negotiation. A missing
        // peer is unreachable; ENOTSUP applies to an actual legacy receiver.
        int const EXEC_ERROR = errno;
        if (EXEC_ERROR != EHOSTUNREACH || !user_ids(0, 0, 0) || !group_ids(0, 0, 0) ||
            getgroups(after_groups.size(), after_groups.data()) != GROUP_COUNT || after_groups != before_groups ||
            restriction_call(CredentialControl::GET_NO_NEW_PRIVILEGES) != 1 ||
            restriction_call(CredentialControl::GET_AUTHORITY_CEILING) != 0) {
            std::fprintf(stderr, "credentials: missing-peer exec error=%d; expected EHOSTUNREACH and unchanged restrictions\n", EXEC_ERROR);
            _exit(1);
        }
        if (process_call(ker::abi::process::procmgmt_ops::SETWKITARGET, reinterpret_cast<uint64_t>(PEER), std::strlen(PEER), 0) != 0)
            _exit(1);
        execl("/usr/bin/testprog", "testprog", "credentials", "restriction-check", hostname.data(), static_cast<char*>(nullptr));
        _exit(1);
    }
    checks.expect(wait_ok(REMOTE), "missing strict peer preserves restrictions and optional exec stays on the same node");

    return checks.finish();
}

auto setid_exec_restrictions(const char* base) -> int {
    Checks checks;
    char path[112];
    std::snprintf(path, sizeof(path), "%s/credential-setid-%ld", base, getpid());
    int const SOURCE = open("/usr/bin/testprog", O_RDONLY);
    int const TARGET = open(path, O_CREAT | O_EXCL | O_WRONLY, 0700);
    bool ready = SOURCE >= 0 && TARGET >= 0;
    std::array<char, 16384> buffer{};
    while (ready) {
        ssize_t const READ = read(SOURCE, buffer.data(), buffer.size());
        if (READ == 0) break;
        if (READ < 0) {
            if (errno == EINTR) continue;
            ready = false;
            break;
        }
        ssize_t offset = 0;
        while (offset < READ) {
            ssize_t const WRITTEN = write(TARGET, buffer.data() + offset, static_cast<size_t>(READ - offset));
            if (WRITTEN < 0 && errno == EINTR) continue;
            if (WRITTEN <= 0) {
                ready = false;
                break;
            }
            offset += WRITTEN;
        }
    }
    if (SOURCE >= 0) close(SOURCE);
    if (TARGET >= 0) close(TARGET);
    ready = ready && chown(path, 0, 0) == 0 && chmod(path, 06755) == 0;
    checks.expect(ready, "prepare disposable set-ID executable");
    if (!ready) {
        unlink(path);
        return checks.finish();
    }
    for (const char* mode : {"ordinary", "nnp", "ceiling"}) {
        pid_t const CHILD = fork();
        if (CHILD == 0) {
            if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
            if (std::strcmp(mode, "nnp") == 0 && restriction_call(CredentialControl::SET_NO_NEW_PRIVILEGES, 1) != 0) _exit(1);
            if (std::strcmp(mode, "ordinary") != 0 && restriction_call(CredentialControl::DROP_AUTHORITY_CEILING, 0) != 0) _exit(1);
            execl(path, "testprog", "credentials", "setid-restriction-check", mode, static_cast<char*>(nullptr));
            _exit(1);
        }
        checks.expect(wait_ok(CHILD), "set-ID exec applies inherited no-new-privileges and authority ceiling");
    }
    checks.expect(unlink(path) == 0, "remove disposable set-ID executable");
    return checks.finish();
}

auto trace_call(ker::abi::ptrace::request request, pid_t pid, uint64_t data = 0) -> int64_t {
    return process_call(ker::abi::process::procmgmt_ops::PTRACE, static_cast<uint64_t>(request), pid, 0, data);
}

auto dumpability(bool value) -> int64_t { return process_call(ker::abi::process::procmgmt_ops::PRCTL, 4, value ? 1 : 0); }

auto trace_policy() -> int {
    using ker::abi::ptrace::request;
    Checks checks;
    std::array<int, 2> ready{};
    std::array<std::array<int, 2>, 3> commands{};
    if (pipe(ready.data()) != 0) return 1;
    for (auto& pipe_fds : commands)
        if (pipe(pipe_fds.data()) != 0) return 1;
    std::array<pid_t, 3> victims{};
    for (unsigned i = 0; i < victims.size(); ++i) {
        victims[i] = fork();
        if (victims[i] == 0) {
            close(ready[0]);
            for (unsigned j = 0; j < commands.size(); ++j) {
                close(commands[j][1]);
                if (j != i) close(commands[j][0]);
            }
            if (setgid(2000) != 0 || setuid(i == 2 ? 1001 : 1000) != 0 || dumpability(i != 1) != 0) _exit(1);
            char value = 'R';
            if (write(ready[1], &value, 1) != 1) _exit(1);
            for (;;) {
                ssize_t result;
                do {
                    result = read(commands[i][0], &value, 1);
                } while (result < 0 && errno == EINTR);
                if (result != 1) _exit(1);
                if (value == 'X') _exit(0);
                if (value == 'D' && dumpability(false) != 0) _exit(1);
                if (value == 'A' && process_call(ker::abi::process::procmgmt_ops::PRCTL, 0x59616d61, getpid()) != 0) _exit(1);
                if (write(ready[1], &value, 1) != 1) _exit(1);
            }
        }
        if (victims[i] < 0) return 1;
    }
    close(ready[1]);
    for (auto& pipe_fds : commands) close(pipe_fds[0]);
    for (unsigned i = 0; i < victims.size(); ++i) {
        char value{};
        checks.expect(read(ready[0], &value, 1) == 1 && value == 'R', "ptrace target ready");
    }
    checks.expect(trace_call(request::SEIZE, victims[1]) == 0 && trace_call(request::DETACH, victims[1]) == 0,
                  "root can trace nondumpable target");
    checks.expect(trace_call(request::SEIZE, victims[2]) == 0, "root attaches before dropping credentials");
    checks.expect(setgid(2000) == 0 && setuid(1000) == 0, "drop tracer identity");
    checks.expect(process_call(ker::abi::process::procmgmt_ops::PRCTL, 3) == 0, "identity change disables process dumping");
    checks.expect(trace_call(request::SETOPTIONS, victims[2], 1) == -EPERM, "existing foreign attachment loses access after drop");
    checks.expect(trace_call(request::GETREGSET, victims[2], 0) == -EPERM, "revoked tracer denied before usercopy");
    checks.expect(trace_call(request::DETACH, victims[2]) == 0, "revoked tracer may release attachment");
    checks.expect(trace_call(request::SEIZE, victims[2]) == -EPERM, "foreign attach denied");
    checks.expect(trace_call(request::SEIZE, victims[1]) == -EPERM, "nondumpable same-user attach denied");
    ker::abi::ptrace::RemoteInfo remote;
    std::memset(&remote, 0xa5, sizeof(remote));
    auto const BEFORE = remote;
    checks.expect(trace_call(request::GET_REMOTE_INFO, victims[1], reinterpret_cast<uint64_t>(&remote)) == -EPERM &&
                      std::memcmp(&remote, &BEFORE, sizeof(remote)) == 0,
                  "private remote-info denied without output changes");
    checks.expect(trace_call(request::SEIZE, victims[0]) == 0, "same-user dumpable attach allowed");
    char value = 'A';
    checks.expect(write(commands[0][1], &value, 1) == 1 && read(ready[0], &value, 1) == 1 && value == 'A',
                  "target sets compatibility ptracer option");
    checks.expect(trace_call(request::SETOPTIONS, victims[0], 0) == 0, "PR_SET_PTRACER cannot replace the active tracer");
    value = 'D';
    checks.expect(write(commands[0][1], &value, 1) == 1 && read(ready[0], &value, 1) == 1 && value == 'D', "target revokes dumpability");
    checks.expect(trace_call(request::SETOPTIONS, victims[0], 1) == -EPERM, "existing attachment rechecks target dumpability");
    checks.expect(trace_call(request::DETACH, victims[0]) == 0, "detach remains available after target revocation");
    checks.expect(trace_call(request::SEIZE, victims[0]) == -EPERM, "target remains protected after detach");
    for (auto& pipe_fds : commands) {
        value = 'X';
        checks.expect(write(pipe_fds[1], &value, 1) == 1, "release ptrace target");
        close(pipe_fds[1]);
    }
    for (pid_t victim : victims) checks.expect(wait_ok(victim), "ptrace target exits cleanly");
    close(ready[0]);
    return checks.finish();
}

auto trace_registers() -> int {
    using namespace ker::abi::ptrace;
    Checks checks;
    std::array<int, 2> ready{};
    if (pipe(ready.data()) != 0) return 1;
    pid_t const VICTIM = fork();
    if (VICTIM == 0) {
        close(ready[0]);
        if (setgid(2000) != 0 || setuid(1000) != 0 || dumpability(true) != 0) _exit(1);
        char const VALUE = 'R';
        if (write(ready[1], &VALUE, 1) != 1) _exit(1);
        for (;;) asm volatile("pause" : : : "memory");
    }
    if (VICTIM < 0) return 1;
    close(ready[1]);
    char value{};
    checks.expect(read(ready[0], &value, 1) == 1 && value == 'R', "register target ready");
    close(ready[0]);
    checks.expect(setgid(2000) == 0 && setuid(1000) == 0, "drop register tracer identity");
    checks.expect(trace_call(request::ATTACH, VICTIM) == 0, "same-user register attach");
    int status{};
    checks.expect(waitpid(VICTIM, &status, WUNTRACED) == VICTIM && WIFSTOPPED(status), "register target stopped");
    auto transfer = [&](request operation, regset kind, void* buffer, size_t size) {
        RegsetIo io{kind, buffer, size};
        int64_t result = -EBUSY;
        for (unsigned attempt = 0; attempt < 1000; ++attempt) {
            result = trace_call(operation, VICTIM, reinterpret_cast<uint64_t>(&io));
            if (result != -EBUSY && result != -EIO) break;
            sched_yield();
        }
        return result;
    };
    X86_64GprState before{};
    bool const HAVE_GPRS = transfer(request::GETREGSET, regset::X86_64_GPR, &before, sizeof(before)) == 0;
    checks.expect(HAVE_GPRS, "read stopped user registers");
    if (HAVE_GPRS) {
        for (unsigned variant = 0; variant < 10; ++variant) {
            auto input = before;
            input.rax ^= 0x1234;
            if (variant == 0) input.rflags |= 3ULL << 12;
            if (variant == 1) input.rflags |= 1ULL << 14;
            if (variant == 2) input.rflags |= 1ULL << 17;
            if (variant == 3) input.rflags &= ~0x200ULL;
            if (variant == 4) input.rip = 0xffff800000001000ULL;
            if (variant == 5) input.rsp = 0;
            if (variant == 6) input.fs_base = 0x800000000000ULL;
            if (variant == 7) input.gs_base = UINT64_MAX;
            if (variant == 8) input.cs = 8;
            if (variant == 9) input.ss = 16;
            X86_64GprState after{};
            checks.expect(transfer(request::SETREGSET, regset::X86_64_GPR, &input, sizeof(input)) == -EINVAL &&
                              transfer(request::GETREGSET, regset::X86_64_GPR, &after, sizeof(after)) == 0 &&
                              std::memcmp(&before, &after, sizeof(before)) == 0,
                          "invalid register write rejected without mutation");
        }
        auto input = before;
        input.rax ^= 0x1234;
        X86_64GprState after{};
        checks.expect(transfer(request::SETREGSET, regset::X86_64_GPR, &input, sizeof(input)) == 0 &&
                          transfer(request::GETREGSET, regset::X86_64_GPR, &after, sizeof(after)) == 0 && after.rax == input.rax,
                      "valid general-register update accepted");
        checks.expect(transfer(request::SETREGSET, regset::X86_64_GPR, &before, sizeof(before)) == 0, "restore original registers");
    }
    std::array<uint8_t, 2688> original{};
    bool const HAVE_XSTATE = transfer(request::GETREGSET, regset::X86_64_XSAVE, original.data(), original.size()) == 0;
    checks.expect(HAVE_XSTATE, "read stopped FPU image");
    if (HAVE_XSTATE) {
        uint32_t eax = 1, ebx{}, ecx{}, edx{};
        asm volatile("cpuid" : "+a"(eax), "=b"(ebx), "=c"(ecx), "=d"(edx));
        unsigned const VARIANTS = (ecx & (1U << 27)) != 0 ? 4 : 1;
        for (unsigned variant = 0; variant < VARIANTS; ++variant) {
            auto input = original;
            if (variant == 0) input[27] |= 0x80;
            if (variant == 1) input[519] |= 0x80;
            if (variant == 2) input[527] |= 0x80;
            if (variant == 3) input[528] = 1;
            std::array<uint8_t, 2688> after{};
            checks.expect(transfer(request::SETREGSET, regset::X86_64_XSAVE, input.data(), input.size()) == -EINVAL &&
                              transfer(request::GETREGSET, regset::X86_64_XSAVE, after.data(), after.size()) == 0 && after == original,
                          "invalid FPU image rejected without mutation");
        }
        checks.expect(transfer(request::SETREGSET, regset::X86_64_XSAVE, original.data(), original.size()) == 0,
                      "valid FPU image accepted");
    }
    if (HAVE_GPRS) {
        HwBreak point{.address = before.rip, .length = 1, .type = hw_break_type::EXECUTE, .slot = 0, .reserved = 0};
        for (unsigned variant = 0; variant < 4; ++variant) {
            auto input = point;
            if (variant == 0) input.address = 0xffff800000001000ULL;
            if (variant == 1) input.type = static_cast<hw_break_type>(2);
            if (variant == 2) input.reserved = 1;
            if (variant == 3) input.length = 8;
            checks.expect(trace_call(request::SET_HW_BREAK, VICTIM, reinterpret_cast<uint64_t>(&input)) == -EINVAL,
                          "invalid hardware breakpoint rejected");
        }
        checks.expect(trace_call(request::SET_HW_BREAK, VICTIM, reinterpret_cast<uint64_t>(&point)) == 0 &&
                          trace_call(request::DEL_HW_BREAK, VICTIM, reinterpret_cast<uint64_t>(&point)) == 0,
                      "valid user breakpoint installs and clears");
    }
    checks.expect(trace_call(request::DETACH, VICTIM) == 0, "detach register target");
    checks.expect(kill(VICTIM, SIGKILL) == 0 && waitpid(VICTIM, &status, 0) == VICTIM && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL,
                  "register target terminates without a kernel fault");
    return checks.finish();
}

auto shared_memory_policy() -> int {
    Checks checks;
    key_t const KEY = static_cast<key_t>(0x19000000U | static_cast<unsigned>(getpid()));
    int const PRIVATE = shmget(KEY, 4096, IPC_CREAT | IPC_EXCL | 0600);
    if (PRIVATE < 0) return 1;
    auto* protected_data = static_cast<volatile uint64_t*>(shmat(PRIVATE, nullptr, 0));
    if (protected_data == reinterpret_cast<void*>(-1)) return 1;
    *protected_data = 0x123456789abcdef0ULL;
    if (setegid(2000) != 0) return 1;
    int const GROUP_READ = shmget(IPC_PRIVATE, 4096, IPC_CREAT | 0040);
    if (setegid(3000) != 0) return 1;
    int const SUPPLEMENTARY = shmget(IPC_PRIVATE, 4096, IPC_CREAT | 0060);
    if (setegid(0) != 0 || GROUP_READ < 0 || SUPPLEMENTARY < 0) return 1;
    pid_t const CHILD = fork();
    if (CHILD == 0) {
        Checks child;
        child.expect(shmdt(const_cast<uint64_t*>(protected_data)) == 0, "detach intentionally inherited private mapping");
        std::array<gid_t, 1> const GROUPS{3000};
        child.expect(setgroups(GROUPS.size(), GROUPS.data()) == 0 && setgid(2000) == 0 && setuid(1000) == 0, "drop SHM caller identity");
        child.expect(shmget(KEY, 0, 0600) == -1 && errno == EACCES, "foreign key read/write lookup denied");
        child.expect(shmget(KEY, 0, 0) == PRIVATE, "zero-access key lookup compatibility");
        child.expect(shmget(KEY, 4096, IPC_CREAT | IPC_EXCL | 0600) == -1 && errno == EEXIST, "exclusive key collision preserved");
        child.expect(shmat(PRIVATE, nullptr, 0) == reinterpret_cast<void*>(-1) && errno == EACCES, "foreign writable attachment denied");
        child.expect(shmat(PRIVATE, nullptr, SHM_RDONLY) == reinterpret_cast<void*>(-1) && errno == EACCES,
                     "foreign readable attachment denied");
        shmid_ds stat;
        std::memset(&stat, 0xa5, sizeof(stat));
        auto const BEFORE = stat;
        child.expect(shmctl(PRIVATE, IPC_STAT, &stat) == -1 && errno == EACCES && std::memcmp(&stat, &BEFORE, sizeof(stat)) == 0,
                     "foreign stat denied without output mutation");
        child.expect(shmctl(PRIVATE, IPC_RMID, nullptr) == -1 && errno == EPERM, "foreign segment removal denied");
        child.expect(shmctl(PRIVATE, IPC_SET, &stat) == -1 && errno == EINVAL, "unsupported IPC_SET remains rejected");
        void* readonly = shmat(GROUP_READ, nullptr, SHM_RDONLY);
        child.expect(readonly != reinterpret_cast<void*>(-1), "primary-group read-only attachment allowed");
        child.expect(shmat(GROUP_READ, nullptr, 0) == reinterpret_cast<void*>(-1) && errno == EACCES,
                     "group read permission cannot grant writable attachment");
        child.expect(
            shmctl(GROUP_READ, IPC_STAT, &stat) == 0 && stat.shm_perm.uid == 0 && stat.shm_perm.gid == 2000 && stat.shm_perm.mode == 0040,
            "stat reports captured owner IDs and permission bits");
        if (readonly != reinterpret_cast<void*>(-1)) child.expect(shmdt(readonly) == 0, "detach group read-only mapping");
        void* grouped = shmat(SUPPLEMENTARY, nullptr, 0);
        child.expect(grouped != reinterpret_cast<void*>(-1), "supplementary group authorizes read/write attachment");
        if (grouped != reinterpret_cast<void*>(-1)) {
            *static_cast<volatile uint64_t*>(grouped) = 456;
            child.expect(shmdt(grouped) == 0, "detach supplementary-group mapping");
        }
        int const OWN = shmget(IPC_PRIVATE, 4096, 0600);
        child.expect(OWN >= 0, "nonroot creates private segment");
        if (OWN >= 0) {
            child.expect(shmctl(OWN, IPC_STAT, &stat) == 0 && stat.shm_perm.uid == 1000 && stat.shm_perm.cuid == 1000 &&
                             stat.shm_perm.gid == 2000 && stat.shm_perm.cgid == 2000 && stat.shm_perm.mode == 0600,
                         "new segment captures complete effective ownership");
            void* owned = shmat(OWN, nullptr, 0);
            child.expect(owned != reinterpret_cast<void*>(-1), "owner attaches read/write");
            child.expect(shmctl(OWN, IPC_RMID, nullptr) == 0, "owner removes own segment");
            if (owned != reinterpret_cast<void*>(-1)) {
                *static_cast<volatile uint64_t*>(owned) = 789;
                child.expect(*static_cast<volatile uint64_t*>(owned) == 789 && shmdt(owned) == 0,
                             "removed segment lives until final detach");
            }
            child.expect(shmctl(OWN, IPC_STAT, &stat) == -1 && errno == EINVAL, "last detach releases removed segment");
        }
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "SHM permission subprocess");
    shmid_ds stat{};
    checks.expect(shmctl(PRIVATE, IPC_STAT, &stat) == 0 && stat.shm_nattch == 1 && stat.shm_perm.uid == 0 && stat.shm_perm.mode == 0600 &&
                      *protected_data == 0x123456789abcdef0ULL,
                  "denied SHM operations preserve segment, attachment count and contents");
    checks.expect(shmdt(const_cast<uint64_t*>(protected_data)) == 0 && shmctl(PRIVATE, IPC_RMID, nullptr) == 0,
                  "root removes protected segment");
    checks.expect(shmctl(GROUP_READ, IPC_RMID, nullptr) == 0 && shmctl(SUPPLEMENTARY, IPC_RMID, nullptr) == 0,
                  "root removes group segments");
    return checks.finish();
}

auto raw_protect(void* address, size_t size, uint64_t prot) -> int64_t {
    uint64_t result = static_cast<uint64_t>(ker::abi::callnums::VMEM);
    register uint64_t protection asm("r8") = prot;
    asm volatile("syscall"
                 : "+a"(result)
                 : "D"(static_cast<uint64_t>(ker::abi::vmem::ops::PROTECT)), "S"(address), "d"(size), "r"(protection)
                 : "rcx", "r11", "memory");
    return static_cast<int64_t>(result);
}

[[noreturn]] void shared_restriction_child(void* address) {
    bool const DENIED = raw_protect(address, 4096, PROT_READ | PROT_WRITE) == -EACCES;
    (void)process_call(ker::abi::process::procmgmt_ops::EXIT, DENIED ? 0 : 1);
    for (;;) asm volatile("pause");
}

auto shared_mapping_rights() -> int {
    Checks checks;
    constexpr size_t PAGE = 4096;
    int const SHM = shmget(IPC_PRIVATE, PAGE, 0600);
    if (SHM < 0) return 1;
    auto* area = static_cast<unsigned char*>(mmap(nullptr, 2 * PAGE, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    if (area == MAP_FAILED || munmap(area, 2 * PAGE) != 0) return 1;
    void* writable = shmat(SHM, area, 0);
    void* readonly = shmat(SHM, area + PAGE, SHM_RDONLY);
    if (writable != area || readonly != area + PAGE) return 1;
    area[0] = 0x5a;
    checks.expect(area[PAGE] == 0x5a, "shared aliases observe initial backing");
    checks.expect(mprotect(readonly, PAGE, PROT_READ | PROT_WRITE) == -1 && errno == EACCES,
                  "read-only attachment cannot gain write permission even as root");
    checks.expect(mprotect(readonly, PAGE, PROT_NONE) == 0 && mprotect(readonly, PAGE, PROT_READ | PROT_WRITE) == -1 && errno == EACCES,
                  "PROT_NONE retains shared write restriction");
    checks.expect(mprotect(readonly, PAGE, PROT_READ) == 0 && area[PAGE] == 0x5a, "read access restores unchanged shared contents");
    checks.expect(mprotect(writable, PAGE, PROT_READ) == 0, "temporarily remove originally granted write access");
    checks.expect(mprotect(area, 2 * PAGE, PROT_READ | PROT_WRITE) == -1 && errno == EACCES,
                  "whole-range upgrade rejects a later restricted leaf");
    std::array<int, 2> pipe_fds{};
    if (pipe(pipe_fds.data()) != 0) return 1;
    unsigned char const INPUT = 0xa5;
    checks.expect(write(pipe_fds[1], &INPUT, 1) == 1 && read(pipe_fds[0], writable, 1) == -1 && errno == EFAULT && area[0] == 0x5a,
                  "denied range upgrade leaves earlier leaf read-only and unchanged");
    close(pipe_fds[0]);
    close(pipe_fds[1]);
    checks.expect(mprotect(writable, PAGE, PROT_READ | PROT_WRITE) == 0, "originally writable attachment regains write access");
    area[0] = 0x66;
    checks.expect(area[PAGE] == 0x66, "read-only alias remains shared after protection changes");
    pid_t const FORKED = fork();
    if (FORKED == 0) {
        bool const OK = setgid(2000) == 0 && setuid(1000) == 0 && mprotect(readonly, PAGE, PROT_NONE) == 0 &&
                        mprotect(readonly, PAGE, PROT_READ | PROT_WRITE) == -1 && errno == EACCES &&
                        mprotect(readonly, PAGE, PROT_READ) == 0 && area[PAGE] == 0x66;
        _exit(OK ? 0 : 1);
    }
    checks.expect(wait_ok(FORKED), "fork retains write restriction after caller identity changes");
    constexpr size_t STACK_SIZE = 64 * 1024;
    auto* stack = static_cast<unsigned char*>(mmap(nullptr, STACK_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    checks.expect(stack != MAP_FAILED, "prepare shared-address-space child stack");
    if (stack != MAP_FAILED) {
        std::array<uint64_t, 7> args{reinterpret_cast<uint64_t>(shared_restriction_child),
                                     reinterpret_cast<uint64_t>(stack + STACK_SIZE - 8),
                                     0x100 | SIGCHLD,
                                     reinterpret_cast<uint64_t>(readonly),
                                     0,
                                     0,
                                     0};
        auto const RESULT = process_call(ker::abi::process::procmgmt_ops::CLONE_VM_PROC, reinterpret_cast<uint64_t>(args.data()));
        checks.expect(RESULT > 0 && wait_ok(static_cast<pid_t>(RESULT)), "CLONE_VM process cannot bypass attachment write limit");
        checks.expect(munmap(stack, STACK_SIZE) == 0, "release shared child stack");
    }
    checks.expect(shmctl(SHM, IPC_RMID, nullptr) == 0, "mark live shared segment for removal");
    checks.expect(mprotect(readonly, PAGE, PROT_READ | PROT_WRITE) == -1 && errno == EACCES && area[PAGE] == 0x66,
                  "segment removal cannot discard live mapping restriction");
    checks.expect(shmdt(writable) == 0, "detach writable alias");
    void* replacement = mmap(readonly, PAGE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, -1, 0);
    checks.expect(replacement == readonly, "MAP_FIXED replaces restricted mapping");
    if (replacement == readonly) {
        area[PAGE] = 0x77;
        checks.expect(
            mprotect(replacement, PAGE, PROT_READ) == 0 && mprotect(replacement, PAGE, PROT_READ | PROT_WRITE) == 0 && area[PAGE] == 0x77,
            "replacement mapping has its own write permission");
    }
    checks.expect(shmdt(readonly) == 0 && area[PAGE] == 0x77, "old SHM detach preserves a replacement mapping");
    checks.expect(munmap(readonly, PAGE) == 0, "release replacement mapping");
    void* reused = mmap(readonly, PAGE, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, -1, 0);
    checks.expect(reused == readonly && mprotect(reused, PAGE, PROT_READ | PROT_WRITE) == 0,
                  "unmap retires restriction before address reuse");
    if (reused != MAP_FAILED) munmap(reused, PAGE);
    return checks.finish();
}

auto metadata_policy(const char* base) -> int {
    Checks checks;
    char directory[96];
    std::snprintf(directory, sizeof(directory), "%s/credentials-metadata-%ld", base, static_cast<long>(getpid()));
    if (mkdir(directory, 0777) != 0 || chmod(directory, 0777) != 0) {
        checks.expect(false, "prepare metadata test directory");
        return checks.finish();
    }
    int const DIR = open(directory, O_RDONLY | O_DIRECTORY);
    if (DIR < 0) return 1;
    int const PROTECTED = openat(DIR, "protected", O_CREAT | O_EXCL | O_RDWR, 0644);
    int const OWNED = openat(DIR, "owned", O_CREAT | O_EXCL | O_RDWR, 0644);
    checks.expect(PROTECTED >= 0 && OWNED >= 0, "create metadata objects");
    struct stat before{};
    bool const READY = PROTECTED >= 0 && OWNED >= 0 && fstat(PROTECTED, &before) == 0 && fchown(OWNED, 1000, 4000) == 0 &&
                       linkat(DIR, "owned", DIR, "alias", 0) == 0 && symlinkat("protected", DIR, "symlink") == 0 &&
                       fchownat(DIR, "symlink", 1000, 2000, AT_SYMLINK_NOFOLLOW) == 0;
    checks.expect(READY, "prepare owner hardlink and separate symlink identity");
    pid_t const CHILD = READY ? fork() : -1;
    if (CHILD == 0) {
        Checks child;
        std::array<gid_t, 1> const GROUPS{3000};
        if (setgroups(GROUPS.size(), GROUPS.data()) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        child.expect(fchmod(PROTECTED, 0777) == -1 && errno == EPERM, "foreign inherited fd chmod denied");
        child.expect(fchown(PROTECTED, 1000, 2000) == -1 && errno == EPERM, "foreign inherited fd chown denied");
        child.expect(fchown(PROTECTED, -1, -1) == -1 && errno == EPERM, "foreign no-op chown denied");
        child.expect(fchmodat(DIR, "protected", 0777, 0) == -1 && errno == EPERM, "foreign dirfd chmod denied");
        child.expect(fchownat(DIR, "protected", 1000, 2000, 0) == -1 && errno == EPERM, "foreign dirfd chown denied");
        char path[128];
        std::snprintf(path, sizeof(path), "%s/protected", directory);
        child.expect(chmod(path, 0777) == -1 && errno == EPERM, "foreign absolute chmod denied");
        child.expect(chown(path, 1000, 2000) == -1 && errno == EPERM, "foreign absolute chown denied");
        struct stat state{};
        child.expect(fchmod(OWNED, 06755) == 0 && fstat(OWNED, &state) == 0 && (state.st_mode & 07777) == 04755,
                     "owner chmod strips nonmember setgid");
        child.expect(fchown(OWNED, -1, 3000) == 0 && fstat(OWNED, &state) == 0 && state.st_gid == 3000 && (state.st_mode & 06000) == 0,
                     "owner may use supplementary group and chown clears set-ID");
        child.expect(fchmodat(DIR, "alias", 06755, 0) == 0 && fstat(OWNED, &state) == 0 && (state.st_mode & 07777) == 06755,
                     "hardlink chmod preserves member setgid on canonical object");
        child.expect(fchown(OWNED, 0, -1) == -1 && errno == EPERM, "owner cannot transfer UID to root");
        child.expect(fchown(OWNED, -1, 4000) == -1 && errno == EPERM, "owner cannot select foreign group");
        child.expect(fstat(OWNED, &state) == 0 && state.st_uid == 1000 && state.st_gid == 3000 && (state.st_mode & 07777) == 06755,
                     "denied ownership changes preserve owner group and set-ID bits");
        child.expect(fchown(OWNED, -1, -1) == 0 && fstat(OWNED, &state) == 0 && (state.st_mode & 06000) == 0,
                     "successful no-op executable chown clears set-ID");
        child.expect(fchownat(DIR, "symlink", -1, 3000, AT_SYMLINK_NOFOLLOW) == 0 &&
                         fstatat(DIR, "symlink", &state, AT_SYMLINK_NOFOLLOW) == 0 && state.st_uid == 1000 && state.st_gid == 3000,
                     "nofollow chown changes owned symlink identity");
        child.expect(fchownat(DIR, "symlink", -1, 3000, 0) == -1 && errno == EPERM,
                     "following symlink denies metadata change to foreign target");
        child.expect(unlinkat(DIR, "owned", 0) == 0 && unlinkat(DIR, "alias", 0) == 0, "remove all names while retaining owned descriptor");
        child.expect(fchmod(OWNED, 0600) == 0 && fstat(OWNED, &state) == 0 && (state.st_mode & 07777) == 0600 && state.st_nlink == 0,
                     "unlinked descriptor retains ownership authorization");
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "metadata owner subprocess");
    struct stat after{};
    checks.expect(PROTECTED >= 0 && fstat(PROTECTED, &after) == 0 && before.st_uid == after.st_uid && before.st_gid == after.st_gid &&
                      before.st_mode == after.st_mode && before.st_ctim.tv_sec == after.st_ctim.tv_sec &&
                      before.st_ctim.tv_nsec == after.st_ctim.tv_nsec,
                  "denied path fd and symlink mutations preserve protected metadata and ctime");
    if (PROTECTED >= 0) close(PROTECTED);
    if (OWNED >= 0) close(OWNED);
    for (const char* name : {"protected", "owned", "alias", "symlink"}) unlinkat(DIR, name, 0);
    close(DIR);
    checks.expect(rmdir(directory) == 0, "remove metadata test directory");
    return checks.finish();
}

auto devfs_metadata_policy() -> int {
    Checks checks;
    int const MASTER = posix_openpt(O_RDWR | O_NOCTTY);
    checks.expect(MASTER >= 0, "allocate metadata PTY");
    if (MASTER < 0) return checks.finish();
    char path[128]{};
    char* name = nullptr;
    bool const PREPARED = grantpt(MASTER) == 0 && unlockpt(MASTER) == 0 && (name = ptsname(MASTER)) != nullptr;
    if (PREPARED) std::snprintf(path, sizeof(path), "%s", name);
    int const OWNED = PREPARED ? open(path, O_RDWR | O_NOCTTY) : -1;
    int const PROTECTED = open("/dev/null", O_RDWR);
    struct stat before{};
    bool const READY = OWNED >= 0 && PROTECTED >= 0 && fstat(PROTECTED, &before) == 0 && fchown(OWNED, 1000, 4000) == 0;
    checks.expect(READY, "prepare retained devfs metadata descriptors");
    pid_t const CHILD = READY ? fork() : -1;
    if (CHILD == 0) {
        Checks child;
        std::array<gid_t, 1> const GROUPS{3000};
        if (setgroups(GROUPS.size(), GROUPS.data()) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        child.expect(fchmod(PROTECTED, 0777) == -1 && errno == EPERM, "foreign device fd chmod denied");
        child.expect(fchown(PROTECTED, 1000, 2000) == -1 && errno == EPERM, "foreign device fd chown denied");
        child.expect(fchown(PROTECTED, -1, -1) == -1 && errno == EPERM, "foreign device no-op chown denied");
        child.expect(chmod("/dev/null", 0777) == -1 && errno == EPERM, "foreign device path chmod denied");
        child.expect(chown("/dev/null", 1000, 2000) == -1 && errno == EPERM, "foreign device path chown denied");
        struct stat state{};
        child.expect(fchmod(OWNED, 06755) == 0 && fstat(OWNED, &state) == 0 && S_ISCHR(state.st_mode) && (state.st_mode & 07777) == 04755,
                     "device owner chmod strips nonmember setgid");
        child.expect(fchown(OWNED, -1, 3000) == 0 && fstat(OWNED, &state) == 0 && state.st_uid == 1000 && state.st_gid == 3000 &&
                         (state.st_mode & 07777) == 04755,
                     "device owner selects supplementary group and preserves device set-ID bits");
        child.expect(chmod(path, 06755) == 0 && fstat(OWNED, &state) == 0 && (state.st_mode & 07777) == 06755,
                     "device path chmod updates retained descriptor");
        child.expect(chown(path, -1, 2000) == 0 && stat(path, &state) == 0 && state.st_uid == 1000 && state.st_gid == 2000,
                     "device path chown honors unchanged UID sentinel");
        child.expect(fchown(OWNED, 0, -1) == -1 && errno == EPERM, "device owner cannot transfer UID to root");
        child.expect(chown(path, -1, 4000) == -1 && errno == EPERM, "device owner cannot choose foreign group");
        child.expect(fstat(OWNED, &state) == 0 && state.st_uid == 1000 && state.st_gid == 2000 && (state.st_mode & 07777) == 06755,
                     "denied device ownership changes preserve complete metadata");
        child.expect(fchmod(OWNED, 0600) == 0 && stat(path, &state) == 0 && (state.st_mode & 07777) == 0600,
                     "device fd chmod updates path metadata");
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "device metadata subprocess");
    struct stat after{};
    checks.expect(PROTECTED >= 0 && fstat(PROTECTED, &after) == 0 && before.st_uid == after.st_uid && before.st_gid == after.st_gid &&
                      before.st_mode == after.st_mode && before.st_ctim.tv_sec == after.st_ctim.tv_sec &&
                      before.st_ctim.tv_nsec == after.st_ctim.tv_nsec,
                  "denied device changes preserve metadata and ctime");
    if (OWNED >= 0) close(OWNED);
    if (PROTECTED >= 0) close(PROTECTED);
    close(MASTER);
    checks.expect(PREPARED && stat(path, &after) == -1 && errno == ENOENT, "metadata PTY removed after last endpoint close");
    return checks.finish();
}

struct MetadataRace {
    const char* path{};
    bool device{};
    std::atomic<bool> start{};
    std::atomic<bool> done{};
    std::atomic<unsigned> ready{};
    std::atomic<unsigned> reads{};
    std::atomic<unsigned> retries{};
    std::atomic<unsigned> failed{};
    int first_result{};
    int first_errno{};
    bool first_path{};
    uid_t first_uid{};
    gid_t first_gid{};
    mode_t first_mode{};
};

struct MetadataRaceReader {
    MetadataRace* state{};
    int fd{};
};

auto metadata_race_reader(void* opaque) -> void* {
    auto& reader = *static_cast<MetadataRaceReader*>(opaque);
    auto& state = *reader.state;
    state.ready.fetch_add(1, std::memory_order_release);
    while (!state.start.load(std::memory_order_acquire)) sched_yield();
    for (unsigned i = 0, observations = 0; observations < 512 || !state.done.load(std::memory_order_acquire); ++i) {
        struct stat value{};
        int const RET = i % 4 == 0 ? stat(state.path, &value) : fstat(reader.fd, &value);
        // Retained path validation has a bounded retry budget. A concurrent
        // XFS transaction may exhaust it; retry without consuming its output.
        if (i % 4 == 0 && RET == -1 && errno == EAGAIN) {
            state.retries.fetch_add(1, std::memory_order_relaxed);
            sched_yield();
            continue;
        }
        ++observations;
        auto const MODE = value.st_mode & 07777;
        bool const VALID = (value.st_uid == 1000 && value.st_gid == 2000 && (MODE == 0755 || MODE == 04755)) ||
                           (value.st_uid == 0 && value.st_gid == 2001 && (MODE == 0755 || (state.device && MODE == 04755)));
        if ((RET != 0 || !VALID) && state.failed.fetch_add(1, std::memory_order_relaxed) == 0) {
            state.first_result = RET;
            state.first_errno = errno;
            state.first_path = i % 4 == 0;
            state.first_uid = value.st_uid;
            state.first_gid = value.st_gid;
            state.first_mode = value.st_mode;
        }
        state.reads.fetch_add(1, std::memory_order_relaxed);
        if (i % 16 == 0) sched_yield();
    }
    return nullptr;
}

auto metadata_snapshot_race(const char* base, const char* existing_path = nullptr) -> int {
    Checks checks;
    char path[96];
    std::snprintf(path, sizeof(path), "%s/credentials-stat-race-%ld", base, static_cast<long>(getpid()));
    if (existing_path != nullptr) std::snprintf(path, sizeof(path), "%s", existing_path);
    int const FD = open(path, existing_path != nullptr ? O_RDWR | O_NOCTTY : O_CREAT | O_EXCL | O_RDWR, 0755);
    if (FD < 0) return 1;
    constexpr int NO_CACHE = 0100000000;  // WOS O_NO_CACHE ABI flag.
    int const UNCACHED = open(path, O_RDONLY | NO_CACHE);
    if (UNCACHED < 0 || fchown(FD, 1000, 2000) != 0 || fchmod(FD, 04755) != 0) return 1;
    MetadataRace state{.path = path, .device = existing_path != nullptr};
    std::array<MetadataRaceReader, 3> readers{{{&state, FD}, {&state, FD}, {&state, UNCACHED}}};
    std::array<pthread_t, 3> threads{};
    unsigned created = 0;
    for (unsigned i = 0; i < readers.size(); ++i) {
        int const ERROR = pthread_create(&threads[i], nullptr, metadata_race_reader, &readers[i]);
        checks.expect(ERROR == 0, "create concurrent metadata reader");
        if (ERROR != 0) break;
        ++created;
    }
    while (state.ready.load(std::memory_order_acquire) != created) sched_yield();
    state.start.store(true, std::memory_order_release);
    unsigned failures = 0;
    for (unsigned i = 0; i < 1000; ++i) {
        // Executable regular files clear set-ID on chown; devices preserve it.
        // Neither backend may publish a mixture of the two owner/group pairs.
        if (fchown(FD, 0, 2001) != 0 || fchown(FD, 1000, 2000) != 0 || fchmod(FD, 04755) != 0) ++failures;
        if (i % 16 == 0) sched_yield();
    }
    state.done.store(true, std::memory_order_release);
    for (unsigned i = 0; i < created; ++i) checks.expect(pthread_join(threads[i], nullptr) == 0, "join concurrent metadata reader");
    checks.expect(failures == 0, "metadata writer transitions succeed");
    std::printf("credentials: metadata race base=%s writes=3000 reads=%u path_retries=%u\n", base, state.reads.load(),
                state.retries.load());
    if (state.failed.load() != 0) {
        std::fprintf(
            stderr, "credentials: metadata race base=%s failures=%u reads=%u first ret=%d errno=%d path=%d uid=%u gid=%u mode=%o\n", base,
            state.failed.load(), state.reads.load(), state.first_result, state.first_errno, state.first_path ? 1 : 0,
            static_cast<unsigned>(state.first_uid), static_cast<unsigned>(state.first_gid), static_cast<unsigned>(state.first_mode));
    }
    checks.expect(state.reads.load() >= created * 512 && state.failed.load() == 0,
                  "cached shared-fd uncached-fd and path stat observe only complete owner mode tuples");
    checks.expect(fchown(FD, 4000, 5000) == 0, "publish final ownership after concurrent readers");
    for (int fd : {FD, UNCACHED}) {
        struct stat value{};
        checks.expect(fstat(fd, &value) == 0 && value.st_uid == 4000 && value.st_gid == 5000 &&
                          (value.st_mode & 07777) == (state.device ? 04755 : 0755),
                      "descriptor stat cache does not resurrect an earlier ownership tuple");
    }
    struct stat value{};
    checks.expect(
        stat(path, &value) == 0 && value.st_uid == 4000 && value.st_gid == 5000 && (value.st_mode & 07777) == (state.device ? 04755 : 0755),
        "path stat cache observes final ownership");
    checks.expect(close(UNCACHED) == 0 && close(FD) == 0 && (existing_path != nullptr || unlink(path) == 0),
                  "release metadata race fixture");
    return checks.finish();
}

auto devfs_metadata_race() -> int {
    int const MASTER = posix_openpt(O_RDWR | O_NOCTTY);
    if (MASTER < 0) return 1;
    int result = 1;
    if (grantpt(MASTER) == 0 && unlockpt(MASTER) == 0) {
        char* path = ptsname(MASTER);
        if (path != nullptr) result = metadata_snapshot_race("/dev/pts", path);
    }
    close(MASTER);
    return result;
}

auto same_times(const struct stat& a, const struct stat& b) -> bool {
    return a.st_atim.tv_sec == b.st_atim.tv_sec && a.st_atim.tv_nsec == b.st_atim.tv_nsec && a.st_mtim.tv_sec == b.st_mtim.tv_sec &&
           a.st_mtim.tv_nsec == b.st_mtim.tv_nsec && a.st_ctim.tv_sec == b.st_ctim.tv_sec && a.st_ctim.tv_nsec == b.st_ctim.tv_nsec;
}

auto timestamp_policy(const char* base) -> int {
    Checks checks;
    bool const DEVICE = base == nullptr;
    char directory[96]{};
    std::array<std::array<char, 128>, 3> paths{};
    std::array<int, 3> files{-1, -1, -1}, masters{-1, -1, -1};
    bool ready = true;
    if (!DEVICE) {
        std::snprintf(directory, sizeof(directory), "%s/credentials-times-%ld", base, static_cast<long>(getpid()));
        ready = mkdir(directory, 0777) == 0 && chmod(directory, 0777) == 0;
    }
    for (size_t i = 0; ready && i < files.size(); ++i) {
        if (DEVICE) {
            masters[i] = posix_openpt(O_RDWR | O_NOCTTY);
            char* name = nullptr;
            ready = masters[i] >= 0 && grantpt(masters[i]) == 0 && unlockpt(masters[i]) == 0 && (name = ptsname(masters[i])) != nullptr;
            if (ready) std::snprintf(paths[i].data(), paths[i].size(), "%s", name);
        } else {
            std::snprintf(paths[i].data(), paths[i].size(), "%s/%zu", directory, i);
        }
        if (ready) files[i] = open(paths[i].data(), (DEVICE ? O_NOCTTY : O_CREAT | O_EXCL) | O_RDWR, 0644);
        ready = ready && files[i] >= 0 && fchmod(files[i], i == 1 ? 0666 : 0644) == 0;
    }
    if (ready) {
        close(files[1]);
        files[1] = open(paths[1].data(), O_RDONLY | O_NOCTTY);
        ready = files[1] >= 0 && fchown(files[2], 1000, 2000) == 0 && fchmod(files[2], 0000) == 0;
    }
    struct stat before{};
    ready = ready && fstat(files[0], &before) == 0;
    checks.expect(ready, "prepare timestamp owner, protected and write-only authority fixtures");
    pid_t const CHILD = ready ? fork() : -1;
    if (CHILD == 0) {
        Checks child;
        if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        std::array<timespec, 2> explicit_times{{{101, 123}, {202, 456}}};
        std::array<timespec, 2> now{{{-99, UTIME_NOW}, {-88, UTIME_NOW}}};
        std::array<timespec, 2> omit{{{-77, UTIME_OMIT}, {-66, UTIME_OMIT}}};
        std::array<timespec, 2> mixed{{{0, UTIME_NOW}, {0, UTIME_OMIT}}};
        child.expect(futimens(files[0], explicit_times.data()) == -1 && errno == EPERM, "foreign fd explicit timestamp denied");
        child.expect(utimensat(AT_FDCWD, paths[0].data(), explicit_times.data(), 0) == -1 && errno == EPERM,
                     "foreign path explicit timestamp denied");
        child.expect(futimens(files[0], nullptr) == -1 && errno == EACCES, "writable fd does not bypass current write mode");
        child.expect(utimensat(AT_FDCWD, paths[0].data(), now.data(), 0) == -1 && errno == EACCES,
                     "foreign path NOW requires current write access");
        child.expect(futimens(files[0], mixed.data()) == -1 && errno == EPERM, "mixed NOW OMIT requires ownership");
        child.expect(futimens(files[0], omit.data()) == 0 && utimensat(AT_FDCWD, paths[0].data(), omit.data(), 0) == 0,
                     "foreign both OMIT is a successful no-op");
        struct stat state{};
        child.expect(fstat(files[0], &state) == 0 && same_times(before, state), "denied and omitted timestamps leave all times unchanged");
        child.expect(futimens(files[1], nullptr) == 0, "read-only fd permits NOW with current write permission");
        child.expect(utimensat(AT_FDCWD, paths[1].data(), now.data(), 0) == 0, "world-writable path permits both NOW");
        child.expect(futimens(files[1], explicit_times.data()) == -1 && errno == EPERM,
                     "write permission does not grant explicit timestamps");
        child.expect(utimensat(AT_FDCWD, paths[1].data(), mixed.data(), 0) == -1 && errno == EPERM,
                     "write permission does not grant mixed NOW OMIT");
        child.expect(futimens(files[2], explicit_times.data()) == 0 && fstat(files[2], &state) == 0 && state.st_atim.tv_sec == 101 &&
                         state.st_atim.tv_nsec == 123 && state.st_mtim.tv_sec == 202 && state.st_mtim.tv_nsec == 456,
                     "owner may set exact timestamps without read or write permission");
        explicit_times[0] = {303, 789};
        explicit_times[1] = {0, UTIME_OMIT};
        child.expect(utimensat(AT_FDCWD, paths[2].data(), explicit_times.data(), 0) == 0 && fstat(files[2], &state) == 0 &&
                         state.st_atim.tv_sec == 303 && state.st_atim.tv_nsec == 789 && state.st_mtim.tv_sec == 202 &&
                         state.st_mtim.tv_nsec == 456,
                     "owner mixed explicit OMIT preserves omitted timestamp");
        auto const SAVED = state;
        explicit_times[1].tv_nsec = 1000000000;
        child.expect(
            futimens(files[2], explicit_times.data()) == -1 && errno == EINVAL && fstat(files[2], &state) == 0 && same_times(SAVED, state),
            "invalid second timestamp never partially updates the first");
        child.expect(futimens(files[2], omit.data()) == 0 && fstat(files[2], &state) == 0 && same_times(SAVED, state),
                     "owner both OMIT does not change ctime");
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "timestamp policy subprocess");
    struct stat after{};
    checks.expect(ready && fstat(files[0], &after) == 0 && same_times(before, after), "protected timestamps unchanged after child exits");
    for (size_t i = 0; i < files.size(); ++i) {
        if (files[i] >= 0) close(files[i]);
        if (masters[i] >= 0) close(masters[i]);
        if (!DEVICE && paths[i][0] != '\0') unlink(paths[i].data());
    }
    if (!DEVICE) rmdir(directory);
    return checks.finish();
}

auto fat_metadata_policy() -> int {
    Checks checks;
    constexpr const char* PATH = "/boot/wos";
    int const FD = open(PATH, O_RDONLY);
    struct stat before{};
    bool const READY = FD >= 0 && fstat(FD, &before) == 0 && S_ISREG(before.st_mode) && before.st_uid == 0 && before.st_gid == 0;
    checks.expect(READY, "open FAT boot file with fixed root ownership");
    if (!READY) {
        if (FD >= 0) close(FD);
        return checks.finish();
    }
    mode_t const MODE = before.st_mode & 07777;
    checks.expect(fchmod(FD, MODE) == 0 && chmod(PATH, MODE) == 0, "authorized unchanged FAT mode succeeds");
    checks.expect(fchown(FD, -1, -1) == 0 && chown(PATH, 0, 0) == 0, "authorized unchanged FAT owner succeeds");
    checks.expect(fchmod(FD, 0600) == -1 && errno == EOPNOTSUPP, "FAT rejects unrepresentable fd mode");
    checks.expect(chmod(PATH, MODE | S_ISUID) == -1 && errno == EOPNOTSUPP, "FAT rejects unrepresentable path set-ID mode");
    checks.expect(fchown(FD, 1000, -1) == -1 && errno == EOPNOTSUPP, "FAT rejects unrepresentable fd owner");
    checks.expect(chown(PATH, -1, 2000) == -1 && errno == EOPNOTSUPP, "FAT rejects unrepresentable path group");
    checks.expect(chmod("/boot", 0755) == 0, "authorized unchanged FAT directory mode succeeds");
    std::array<timespec, 2> explicit_times{{{101, 123}, {202, 456}}};
    std::array<timespec, 2> omit{{{-99, UTIME_OMIT}, {-88, UTIME_OMIT}}};
    checks.expect(futimens(FD, nullptr) == -1 && errno == EOPNOTSUPP, "FAT does not fake a successful NOW update");
    checks.expect(utimensat(AT_FDCWD, PATH, explicit_times.data(), 0) == -1 && errno == EOPNOTSUPP,
                  "FAT rejects unsupported explicit timestamps");
    checks.expect(futimens(FD, omit.data()) == 0, "FAT supports unchanged timestamp request");
    pid_t const CHILD = fork();
    if (CHILD == 0) {
        Checks child;
        if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        child.expect(fchmod(FD, MODE) == -1 && errno == EPERM, "foreign FAT no-op fd chmod denied");
        child.expect(fchown(FD, -1, -1) == -1 && errno == EPERM, "foreign FAT no-op fd chown denied");
        child.expect(chmod(PATH, MODE) == -1 && errno == EPERM, "foreign FAT no-op path chmod denied");
        child.expect(chown(PATH, -1, -1) == -1 && errno == EPERM, "foreign FAT no-op path chown denied");
        child.expect(chmod("/boot", 0755) == -1 && errno == EPERM, "foreign FAT directory chmod denied");
        child.expect(chown("/boot", -1, -1) == -1 && errno == EPERM, "foreign FAT directory chown denied");
        child.expect(futimens(FD, explicit_times.data()) == -1 && errno == EPERM, "foreign FAT explicit timestamp denied");
        child.expect(utimensat(AT_FDCWD, PATH, nullptr, 0) == -1 && errno == EACCES, "foreign FAT NOW requires write access");
        child.expect(futimens(FD, omit.data()) == 0 && utimensat(AT_FDCWD, PATH, omit.data(), 0) == 0,
                     "foreign FAT both OMIT is an authorized no-op");
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "FAT metadata denial subprocess");
    struct stat after{};
    checks.expect(fstat(FD, &after) == 0 && before.st_uid == after.st_uid && before.st_gid == after.st_gid &&
                      before.st_mode == after.st_mode && before.st_ctim.tv_sec == after.st_ctim.tv_sec &&
                      before.st_ctim.tv_nsec == after.st_ctim.tv_nsec,
                  "FAT metadata unchanged after denied and no-op fd requests");
    checks.expect(
        stat(PATH, &after) == 0 && before.st_uid == after.st_uid && before.st_gid == after.st_gid && before.st_mode == after.st_mode,
        "FAT path and descriptor metadata agree");
    char missing[96];
    std::snprintf(missing, sizeof(missing), "/boot/credentials-missing-%ld", static_cast<long>(getpid()));
    checks.expect(chmod(missing, 0644) == -1 && errno == ENOENT, "FAT chmod verifies target existence");
    checks.expect(chown(missing, 0, 0) == -1 && errno == ENOENT, "FAT chown verifies target existence");
    checks.expect(fstat(FD, &after) == 0 && same_times(before, after), "FAT requests never fake timestamp changes");
    close(FD);
    return checks.finish();
}

auto xattr_policy() -> int {
    Checks checks;
    char directory[96], protected_path[128], owned_path[128], link_path[128];
    std::snprintf(directory, sizeof(directory), "/credentials-xattr-%ld", static_cast<long>(getpid()));
    std::snprintf(protected_path, sizeof(protected_path), "%s/protected", directory);
    std::snprintf(owned_path, sizeof(owned_path), "%s/owned", directory);
    std::snprintf(link_path, sizeof(link_path), "%s/link", directory);
    bool ready = mkdir(directory, 0777) == 0 && chmod(directory, 0777) == 0;
    int const PROTECTED = ready ? open(protected_path, O_CREAT | O_EXCL | O_RDWR, 0600) : -1;
    int owned = ready ? open(owned_path, O_CREAT | O_EXCL | O_RDWR, 0644) : -1;
    constexpr char VALUE[] = "original";
    ready = PROTECTED >= 0 && owned >= 0 && fsetxattr(PROTECTED, "user.value", VALUE, sizeof(VALUE), 0) == 0 &&
            fsetxattr(owned, "user.value", VALUE, sizeof(VALUE), 0) == 0 &&
            fsetxattr(owned, "trusted.hidden", VALUE, sizeof(VALUE), 0) == 0 &&
            fsetxattr(owned, "security.hidden", VALUE, sizeof(VALUE), 0) == 0 && fchown(owned, 1000, 2000) == 0 &&
            symlink("owned", link_path) == 0 && lchown(link_path, 1000, 2000) == 0;
    if (ready) {
        close(owned);
        owned = open(owned_path, O_RDONLY);
        ready = owned >= 0;
    }
    struct stat before{};
    ready = ready && fstat(PROTECTED, &before) == 0;
    checks.expect(ready, "prepare XFS xattr permission and namespace fixtures");
    pid_t const CHILD = ready ? fork() : -1;
    if (CHILD == 0) {
        Checks child;
        if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        std::array<char, 256> output{};
        output.fill('!');
        child.expect(fgetxattr(PROTECTED, "user.value", output.data(), output.size()) == -1 && errno == EACCES && output[0] == '!',
                     "foreign unreadable inherited fd xattr read denied without output");
        child.expect(getxattr(protected_path, "user.value", output.data(), output.size()) == -1 && errno == EACCES && output[0] == '!',
                     "foreign unreadable path xattr read denied without output");
        child.expect(flistxattr(PROTECTED, output.data(), output.size()) == -1 && errno == EACCES && output[0] == '!',
                     "foreign unreadable xattr list denied without output");
        child.expect(fsetxattr(PROTECTED, "user.value", "changed", 8, 0) == -1 && errno == EACCES,
                     "inherited writable fd does not bypass xattr write mode");
        child.expect(setxattr(protected_path, "user.new", "changed", 8, 0) == -1 && errno == EACCES, "foreign xattr path creation denied");
        child.expect(fremovexattr(PROTECTED, "user.value") == -1 && errno == EACCES, "foreign xattr fd removal denied");
        child.expect(removexattr(protected_path, "user.value") == -1 && errno == EACCES, "foreign xattr path removal denied");
        child.expect(fgetxattr(owned, "user.value", output.data(), output.size()) == sizeof(VALUE) &&
                         std::memcmp(output.data(), VALUE, sizeof(VALUE)) == 0,
                     "owner reads user xattr through inherited descriptor");
        for (const char* name : {"trusted.hidden", "security.hidden"}) {
            output.fill('!');
            child.expect(fgetxattr(owned, name, output.data(), output.size()) == -1 && errno == EPERM && output[0] == '!',
                         "ownership does not grant privileged xattr read");
            child.expect(setxattr(owned_path, name, "changed", 8, 0) == -1 && errno == EPERM,
                         "ownership does not grant privileged xattr write");
            child.expect(fremovexattr(owned, name) == -1 && errno == EPERM, "ownership does not grant privileged xattr removal");
        }
        output.fill('!');
        ssize_t const COUNT = flistxattr(owned, output.data(), output.size());
        child.expect(COUNT == sizeof("user.value") && std::memcmp(output.data(), "user.value", sizeof("user.value")) == 0,
                     "one snapshot filters privileged names from xattr listing");
        child.expect(listxattr(owned_path, nullptr, 0) == COUNT, "filtered xattr size query agrees with descriptor list");
        child.expect(fsetxattr(owned, "user.value", "changed", 8, XATTR_REPLACE) == 0,
                     "current mode permits user xattr write on read-only descriptor");
        child.expect(lsetxattr(link_path, "user.blocked", VALUE, sizeof(VALUE), 0) == -1 && errno == EPERM,
                     "user xattr mutation on symlink denied");
        child.expect(fchmod(owned, 0400) == 0 && fsetxattr(owned, "user.value", VALUE, sizeof(VALUE), 0) == -1 && errno == EACCES,
                     "owner xattr write honors revoked write mode");
        child.expect(fchmod(owned, 0600) == 0 && fremovexattr(owned, "user.value") == 0, "restored mode permits owner xattr removal");
        struct stat state{};
        child.expect(fstat(PROTECTED, &state) == 0 && same_times(before, state), "denied xattr requests preserve protected metadata");
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "xattr authorization subprocess");
    std::array<char, 128> output{};
    checks.expect(ready && fgetxattr(PROTECTED, "user.value", output.data(), output.size()) == sizeof(VALUE) &&
                      std::memcmp(output.data(), VALUE, sizeof(VALUE)) == 0,
                  "denied xattr replacement and removal leave original value");
    for (const char* name : {"trusted.hidden", "security.hidden"}) {
        checks.expect(ready && fgetxattr(owned, name, output.data(), output.size()) == sizeof(VALUE) &&
                          std::memcmp(output.data(), VALUE, sizeof(VALUE)) == 0,
                      "denied privileged xattr operations leave original value");
    }
    if (PROTECTED >= 0) close(PROTECTED);
    if (owned >= 0) close(owned);
    unlink(link_path);
    unlink(protected_path);
    unlink(owned_path);
    rmdir(directory);
    return checks.finish();
}

auto file_open_policy(const char* base) -> int {
    Checks checks;
    char directory[96];
    std::snprintf(directory, sizeof(directory), "%s/credentials-open-%ld", base, static_cast<long>(getpid()));
    bool ready = mkdir(directory, 0777) == 0 && chmod(directory, 0777) == 0;
    int const DIR = ready ? open(directory, O_RDONLY | O_DIRECTORY) : -1;
    int const PROTECTED = DIR >= 0 ? openat(DIR, "protected", O_CREAT | O_EXCL | O_RDWR, 0600) : -1;
    int const READABLE = DIR >= 0 ? openat(DIR, "readable", O_CREAT | O_EXCL | O_RDWR, 0644) : -1;
    constexpr char CONTENT[] = "must survive denied open";
    ready = PROTECTED >= 0 && READABLE >= 0 && write(PROTECTED, CONTENT, sizeof(CONTENT)) == sizeof(CONTENT) &&
            write(READABLE, CONTENT, sizeof(CONTENT)) == sizeof(CONTENT) && fchmod(READABLE, 0444) == 0 &&
            linkat(DIR, "protected", DIR, "alias", 0) == 0 && mkdirat(DIR, "blocked", 0755) == 0 &&
            fchmodat(DIR, "blocked", 0555, 0) == 0 && mkdirat(DIR, "setgid", 0777) == 0 && fchownat(DIR, "setgid", 0, 4000, 0) == 0 &&
            fchmodat(DIR, "setgid", 02777, 0) == 0;
    struct stat before_protected{}, before_readable{}, before_blocked{};
    ready = ready && fstat(PROTECTED, &before_protected) == 0 && fstat(READABLE, &before_readable) == 0 &&
            fstatat(DIR, "blocked", &before_blocked, 0) == 0;
    checks.expect(ready, "prepare open denial and creator fixtures");
    pid_t const CHILD = ready ? fork() : -1;
    if (CHILD == 0) {
        Checks child;
        std::array<gid_t, 1> const GROUPS{3000};
        if (setgroups(GROUPS.size(), GROUPS.data()) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        umask(0027);
        for (const char* name : {"protected", "alias"}) {
            for (int flags : {O_WRONLY | O_TRUNC, O_RDWR | O_CREAT | O_TRUNC}) {
                int const FD = openat(DIR, name, flags, 0777);
                child.expect(FD == -1 && errno == EACCES, "foreign truncating open denied before contents change");
                if (FD >= 0) close(FD);
            }
        }
        int fd = openat(DIR, "protected", O_RDONLY);
        child.expect(fd == -1 && errno == EACCES, "foreign private file read denied");
        if (fd >= 0) close(fd);
        fd = openat(DIR, "readable", O_RDONLY | O_TRUNC);
        child.expect(fd == -1 && errno == EACCES, "read-only truncating open requires write authority");
        if (fd >= 0) close(fd);
        fd = openat(DIR, "readable", O_CREAT | O_EXCL | O_TRUNC | O_WRONLY, 0644);
        child.expect(fd == -1 && errno == EEXIST, "exclusive create leaves existing readable file intact");
        if (fd >= 0) close(fd);
        fd = openat(DIR, "blocked/new", O_CREAT | O_RDWR, 0666);
        child.expect(fd == -1 && errno == EACCES, "directory write denial prevents creation");
        if (fd >= 0) close(fd);
        fd = openat(DIR, "missing/leaf", O_CREAT | O_RDWR, 0666);
        child.expect(fd == -1 && errno == ENOENT, "open does not create missing ancestor directories");
        if (fd >= 0) close(fd);
        fd = openat(DIR, "created", O_CREAT | O_EXCL | O_RDWR, 0666);
        struct stat state{};
        child.expect(fd >= 0 && fstat(fd, &state) == 0 && state.st_uid == 1000 && state.st_gid == 2000 && (state.st_mode & 07777) == 0640,
                     "creation uses effective identity and umask");
        if (fd >= 0) close(fd);
        fd = openat(DIR, "zero", O_CREAT | O_EXCL | O_RDWR, 0000);
        child.expect(fd >= 0 && write(fd, CONTENT, sizeof(CONTENT)) == sizeof(CONTENT) && fstat(fd, &state) == 0 && state.st_uid == 1000 &&
                         (state.st_mode & 07777) == 0,
                     "new mode-zero file returns usable creation descriptor");
        int const REOPEN = openat(DIR, "zero", O_RDONLY);
        child.expect(REOPEN == -1 && errno == EACCES, "new mode applies to subsequent opens");
        if (REOPEN >= 0) close(REOPEN);
        if (fd >= 0) close(fd);
        fd = openat(DIR, "setgid/created", O_CREAT | O_EXCL | O_RDWR, 02666);
        child.expect(fd >= 0 && fstat(fd, &state) == 0 && state.st_uid == 1000 && state.st_gid == 4000 && (state.st_mode & 07777) == 0640,
                     "setgid parent supplies group without granting nonmember setgid bit");
        if (fd >= 0) close(fd);
        child.expect(fstat(PROTECTED, &state) == 0 && state.st_size == before_protected.st_size && same_times(state, before_protected),
                     "denied opens preserve protected inode size and all timestamps");
        child.expect(fstat(READABLE, &state) == 0 && state.st_size == before_readable.st_size && same_times(state, before_readable),
                     "denied and exclusive opens preserve readable inode");
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "open authorization subprocess");
    std::array<char, sizeof(CONTENT)> readback{};
    checks.expect(ready && pread(PROTECTED, readback.data(), readback.size(), 0) == sizeof(CONTENT) &&
                      std::memcmp(readback.data(), CONTENT, sizeof(CONTENT)) == 0,
                  "protected contents survive denied truncation through hardlinks");
    checks.expect(ready && pread(READABLE, readback.data(), readback.size(), 0) == sizeof(CONTENT) &&
                      std::memcmp(readback.data(), CONTENT, sizeof(CONTENT)) == 0,
                  "readable contents survive denied read-only truncation");
    struct stat state{};
    checks.expect(ready && fstatat(DIR, "blocked/new", &state, 0) == -1 && errno == ENOENT && fstatat(DIR, "blocked", &state, 0) == 0 &&
                      same_times(state, before_blocked),
                  "denied creation leaves no name or parent timestamp change");
    if (PROTECTED >= 0) close(PROTECTED);
    if (READABLE >= 0) close(READABLE);
    if (DIR >= 0) {
        for (const char* name : {"protected", "readable", "alias", "created", "zero", "setgid/created"}) unlinkat(DIR, name, 0);
        unlinkat(DIR, "blocked", AT_REMOVEDIR);
        unlinkat(DIR, "setgid", AT_REMOVEDIR);
        close(DIR);
    }
    rmdir(directory);
    return checks.finish();
}

auto fat_open_policy() -> int {
    Checks checks;
    constexpr const char* PATH = "/boot/wos";
    char missing[96];
    std::snprintf(missing, sizeof(missing), "/boot/credentials-denied-%ld", static_cast<long>(getpid()));
    int const FD = open(PATH, O_RDONLY);
    struct stat before{};
    std::array<char, 64> original{};
    bool const READY = FD >= 0 && fstat(FD, &before) == 0 && pread(FD, original.data(), original.size(), 0) == original.size();
    checks.expect(READY, "prepare FAT open denial fixture");
    pid_t const CHILD = READY ? fork() : -1;
    if (CHILD == 0) {
        Checks child;
        if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        int fd = open(PATH, O_RDONLY);
        std::array<char, 64> contents{};
        child.expect(fd >= 0 && read(fd, contents.data(), contents.size()) == contents.size() && contents == original,
                     "nonroot FAT read remains usable");
        if (fd >= 0) close(fd);
        for (int flags : {O_WRONLY, O_RDWR, O_WRONLY | O_TRUNC, O_RDONLY | O_TRUNC, O_RDWR | O_CREAT | O_TRUNC}) {
            fd = open(PATH, flags, 0600);
            child.expect(fd == -1 && errno == EACCES, "nonroot FAT write or truncating open denied");
            if (fd >= 0) close(fd);
        }
        fd = open(missing, O_CREAT | O_EXCL | O_RDWR, 0600);
        child.expect(fd == -1 && errno == EACCES, "fixed root-owned FAT parent denies nonroot creation");
        if (fd >= 0) close(fd);
        fd = open(PATH, O_CREAT | O_EXCL | O_WRONLY | O_TRUNC, 0644);
        child.expect(fd == -1 && errno == EEXIST, "exclusive FAT create preserves existing file");
        if (fd >= 0) close(fd);
        fd = open(PATH, O_RDONLY | O_DIRECTORY);
        child.expect(fd == -1 && errno == ENOTDIR, "FAT regular file cannot open as directory");
        if (fd >= 0) close(fd);
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "FAT open denial subprocess");
    struct stat after{};
    std::array<char, 64> contents{};
    checks.expect(READY && stat(PATH, &after) == 0 && before.st_size == after.st_size && same_times(before, after) &&
                      pread(FD, contents.data(), contents.size(), 0) == contents.size() && contents == original,
                  "denied FAT opens preserve file size timestamps and contents");
    checks.expect(stat(missing, &after) == -1 && errno == ENOENT, "denied FAT creation leaves no directory entry");
    unlink(missing);
    if (FD >= 0) close(FD);
    return checks.finish();
}

auto devfs_open_policy() -> int {
    Checks checks;
    int const MASTER = posix_openpt(O_RDWR | O_NOCTTY | O_NONBLOCK);
    std::array<char, 96> path{};
    char* name = nullptr;
    bool ready = MASTER >= 0 && grantpt(MASTER) == 0 && unlockpt(MASTER) == 0 && (name = ptsname(MASTER)) != nullptr;
    if (ready) std::snprintf(path.data(), path.size(), "%s", name);
    int const SLAVE = ready ? open(path.data(), O_RDWR | O_NOCTTY) : -1;
    ready = ready && SLAVE >= 0 && fchmod(SLAVE, 0600) == 0;
    checks.expect(ready, "prepare private PTY open fixture");
    pid_t const CHILD = ready ? fork() : -1;
    if (CHILD == 0) {
        Checks child;
        if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        for (int flags : {O_RDONLY, O_WRONLY, O_RDWR, O_RDONLY | O_TRUNC}) {
            int const FD = open(path.data(), flags | O_NOCTTY);
            child.expect(FD == -1 && errno == EACCES, "private PTY rejects nonroot reopen before device callback");
            if (FD >= 0) close(FD);
        }
        int fd = open("/dev/ptmx", O_RDONLY | O_DIRECTORY);
        child.expect(fd == -1 && errno == ENOTDIR, "device directory requirement rejects before allocating PTY");
        if (fd >= 0) close(fd);
        fd = open("/dev/ptmx", O_CREAT | O_EXCL | O_RDWR, 0600);
        child.expect(fd == -1 && errno == EEXIST, "exclusive device open rejects before allocating PTY");
        if (fd >= 0) close(fd);
        std::array<char, 64> link{};
        child.expect(readlink("/dev/ptmx", link.data(), link.size()) == -1 && errno == EINVAL,
                     "readlink on device returns EINVAL without opening it");
        fd = open("/dev/null", O_RDWR);
        child.expect(fd >= 0 && write(fd, "ok", 2) == 2, "public null device remains usable for nonroot");
        if (fd >= 0) close(fd);
        child.expect(write(SLAVE, "ok", 2) == 2, "inherited open PTY rights survive later credential change");
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "devfs open denial subprocess");
    std::array<char, 2> contents{};
    checks.expect(ready && read(MASTER, contents.data(), contents.size()) == 2 && contents[0] == 'o' && contents[1] == 'k',
                  "authorized inherited PTY writes still reach master");
    if (SLAVE >= 0) close(SLAVE);
    if (MASTER >= 0) close(MASTER);
    return checks.finish();
}

auto procfs_global_policy() -> int {
    Checks checks;
    constexpr std::array<const char*, 10> CONTROL_PATHS{"/proc/kperfctl",
                                                        "/proc/wki/chaos",
                                                        "/proc/wki/chaos_workload",
                                                        "/proc/memacc/track/page_callers",
                                                        "/proc/memacc/track/kmalloc_debug",
                                                        "/proc/memacc/reclaim/buffer_cache",
                                                        "/proc/memacc/reclaim/packet_pool",
                                                        "/proc/memacc/reclaim/xfs_inode",
                                                        "/proc/memacc/reclaim/file_mmap_cache",
                                                        "/proc/memacc/reclaim/anonymous_swap"};
    std::array<int, CONTROL_PATHS.size()> descriptors{};
    descriptors.fill(-1);
    std::array<char, 32> before{};
    ssize_t before_len = -1;
    bool ready = true;
    for (size_t i = 0; i < CONTROL_PATHS.size(); ++i) {
        descriptors[i] = open(CONTROL_PATHS[i], O_RDWR);
        std::array<char, 32> content{};
        ssize_t const LENGTH = descriptors[i] >= 0 ? pread(descriptors[i], content.data(), content.size(), 0) : -1;
        checks.expect(descriptors[i] >= 0 && LENGTH > 0, "root can open and prime global procfs control content");
        ready &= descriptors[i] >= 0 && LENGTH > 0;
        if (i == 0) {
            before = content;
            before_len = LENGTH;
        }
    }
    pid_t const CHILD = ready ? fork() : -1;
    if (CHILD == 0) {
        Checks child;
        if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        for (size_t i = 0; i < CONTROL_PATHS.size(); ++i) {
            for (int flags : {O_RDONLY, O_WRONLY, O_RDWR, O_RDONLY | O_TRUNC}) {
                int const FD = open(CONTROL_PATHS[i], flags);
                child.expect(FD == -1 && errno == EACCES, "nonroot cannot open global procfs controls");
                if (FD >= 0) close(FD);
            }
            std::array<char, 4> output{'s', 'a', 'f', 'e'};
            child.expect(pread(descriptors[i], output.data(), output.size(), 0) == -1 && errno == EACCES &&
                             std::memcmp(output.data(), "safe", 4) == 0,
                         "inherited primed procfs read denies after privilege loss without output");
            for (const char* command : {"enable", "disable", "clear", "reset", "drop", "status"}) {
                child.expect(write(descriptors[i], command, std::strlen(command)) == -1 && errno == EACCES,
                             "inherited procfs controls deny commands after privilege loss");
            }
            struct stat st{};
            child.expect(stat(CONTROL_PATHS[i], &st) == 0 && (st.st_mode & 0777) == 0600,
                         "protected control metadata remains available with root-only mode");
        }
        for (const char* path : {"/proc/kperf", "/proc/kwkistat", "/proc/kcpustate", "/proc/kcontstat", "/proc/kipcstat",
                                 "/proc/wki/netdiag", "/proc/wki/pipes", "/proc/memacc/procs", "/proc/memacc/dead", "/proc/memacc/slabs",
                                 "/proc/memacc/kmalloc_live", "/proc/memacc/kmalloc_callers", "/proc/memacc/page_callers"}) {
            int const FD = open(path, O_RDONLY);
            child.expect(FD == -1 && errno == EACCES, "global kernel diagnostics deny nonroot reads");
            if (FD >= 0) close(FD);
            struct stat st{};
            child.expect(stat(path, &st) == 0 && (st.st_mode & 0777) == 0400, "protected diagnostic metadata reports root-only read mode");
        }
        for (const char* path : {"/proc/version", "/proc/stat", "/proc/meminfo", "/proc/memacc/summary"}) {
            int const FD = open(path, O_RDONLY);
            std::array<char, 32> output{};
            child.expect(FD >= 0 && read(FD, output.data(), output.size()) > 0, "public procfs aggregates remain readable");
            if (FD >= 0) close(FD);
        }
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "procfs global authority subprocess");
    int const FRESH = open("/proc/kperfctl", O_RDONLY);
    std::array<char, 32> after{};
    ssize_t const AFTER_LEN = FRESH >= 0 ? read(FRESH, after.data(), after.size()) : -1;
    checks.expect(before_len > 0 && AFTER_LEN == before_len && before == after,
                  "denied inherited commands leave global perf recording state unchanged");
    if (FRESH >= 0) close(FRESH);
    for (int fd : descriptors)
        if (fd >= 0) close(fd);
    return checks.finish();
}

auto procfs_fd_policy() -> int {
    Checks checks;
    std::array<char, 128> file_path{};
    std::snprintf(file_path.data(), file_path.size(), "/tmp/credentials-proc-fd-%ld", static_cast<long>(getpid()));
    int const ORIGINAL = open(file_path.data(), O_CREAT | O_EXCL | O_RDWR, 0644);
    if (ORIGINAL < 0 || write(ORIGINAL, "keep", 4) != 4) return 1;
    pid_t const PARENT = getpid();
    std::array<char, 128> fd_dir{};
    std::snprintf(fd_dir.data(), fd_dir.size(), "/proc/%ld/fd", static_cast<long>(PARENT));
    int const DIRECTORY = open(fd_dir.data(), O_RDONLY | O_DIRECTORY);
    pid_t const CHILD = fork();
    if (CHILD == 0) {
        Checks child;
        close(ORIGINAL);
        if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        for (bool thread_alias : {false, true}) {
            std::array<char, 160> path{};
            if (thread_alias)
                std::snprintf(path.data(), path.size(), "/proc/%ld/task/%ld/fd/%d", static_cast<long>(PARENT), static_cast<long>(PARENT),
                              ORIGINAL);
            else
                std::snprintf(path.data(), path.size(), "/proc/%ld/fd/%d", static_cast<long>(PARENT), ORIGINAL);
            for (int flags : {O_RDONLY, O_WRONLY, O_RDWR}) {
                int fd = open(path.data(), flags);
                child.expect(fd == -1 && errno == EACCES, "foreign procfs descriptor open denies without path fallback");
                if (fd >= 0) close(fd);
                fd = openat(AT_FDCWD, path.data(), flags);
                child.expect(fd == -1 && errno == EACCES, "foreign procfs openat cannot bypass descriptor policy through readlink");
                if (fd >= 0) close(fd);
            }
            std::array<char, 4> output{'s', 'a', 'f', 'e'};
            child.expect(
                readlink(path.data(), output.data(), output.size()) == -1 && errno == EACCES && std::memcmp(output.data(), "safe", 4) == 0,
                "denied foreign FD readlink leaves output unchanged");
        }
        std::array<char, 24> component{};
        std::snprintf(component.data(), component.size(), "%d", ORIGINAL);
        int const FD = openat(DIRECTORY, component.data(), O_RDONLY);
        child.expect(FD == -1 && errno == EACCES, "inherited foreign FD directory does not confer descriptor access");
        if (FD >= 0) close(FD);
        int const PUBLIC = open(file_path.data(), O_RDONLY);
        child.expect(PUBLIC >= 0, "ordinary public file access remains independently allowed");
        if (PUBLIC >= 0) close(PUBLIC);
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "foreign descriptor denial subprocess");
    std::array<char, 4> contents{};
    checks.expect(pread(ORIGINAL, contents.data(), contents.size(), 0) == 4 && std::memcmp(contents.data(), "keep", 4) == 0,
                  "denied descriptor access preserves target contents");
    checks.expect(fchmod(ORIGINAL, 0600) == 0, "make target pathname inaccessible before descriptor transfer tests");
    std::array<int, 2> ready{}, commands{};
    if (pipe(ready.data()) != 0 || pipe(commands.data()) != 0) return 1;
    pid_t const TARGET = fork();
    if (TARGET == 0) {
        close(ready[0]);
        close(commands[1]);
        if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0 || dumpability(true) != 0) _exit(1);
        char value = 'R';
        if (write(ready[1], &value, 1) != 1 || read(commands[0], &value, 1) != 1 || dumpability(false) != 0 ||
            write(ready[1], &value, 1) != 1 || read(commands[0], &value, 1) != 1)
            _exit(1);
        _exit(0);
    }
    close(ready[1]);
    close(commands[0]);
    char value{};
    checks.expect(read(ready[0], &value, 1) == 1 && value == 'R', "dumpable equal-identity descriptor target ready");
    checks.expect(setgroups(0, nullptr) == 0 && setgid(2000) == 0 && setuid(1000) == 0, "drop descriptor caller identity");
    std::array<char, 128> target_path{};
    std::snprintf(target_path.data(), target_path.size(), "/proc/%ld/fd/%d", static_cast<long>(TARGET), ORIGINAL);
    int const PATH_FD = open(file_path.data(), O_RDONLY);
    checks.expect(PATH_FD == -1 && errno == EACCES, "protected pathname cannot substitute for descriptor transfer");
    if (PATH_FD >= 0) close(PATH_FD);
    int fd = open(target_path.data(), O_RDONLY);
    checks.expect(fd >= 0 && pread(fd, contents.data(), contents.size(), 0) == 4, "dumpable equal-identity descriptor access succeeds");
    if (fd >= 0) close(fd);
    value = 'D';
    checks.expect(write(commands[1], &value, 1) == 1 && read(ready[0], &value, 1) == 1, "target revokes dumpability");
    fd = open(target_path.data(), O_RDONLY);
    checks.expect(fd == -1 && errno == EACCES, "nondumpable equal-identity target rejects subsequent descriptor transfer");
    if (fd >= 0) close(fd);
    std::array<char, 128> self_path{};
    std::snprintf(self_path.data(), self_path.size(), "/proc/self/fd/%d", ORIGINAL);
    fd = open(self_path.data(), O_RDONLY);
    checks.expect(fd >= 0 && pread(fd, contents.data(), contents.size(), 0) == 4, "same-process descriptor access survives nondumpability");
    if (fd >= 0) close(fd);
    std::array<char, 128> link{};
    checks.expect(readlink(self_path.data(), link.data(), link.size()) > 0, "same-process descriptor link remains available");
    std::array<int, 2> stream{};
    checks.expect(pipe(stream.data()) == 0, "create anonymous descriptor fixture");
    std::snprintf(self_path.data(), self_path.size(), "/proc/self/task/%ld/fd/%d", static_cast<long>(getpid()), stream[0]);
    fd = open(self_path.data(), O_RDONLY);
    char first{}, second{};
    checks.expect(fd >= 0 && write(stream[1], "ab", 2) == 2 && read(fd, &first, 1) == 1 && first == 'a' &&
                      read(stream[0], &second, 1) == 1 && second == 'b',
                  "task-alias reopen retains the actual anonymous pipe");
    if (fd >= 0) close(fd);
    fd = open(self_path.data(), O_RDONLY | O_NOFOLLOW);
    checks.expect(fd == -1 && errno == ELOOP, "nofollow still rejects final descriptor link");
    if (fd >= 0) close(fd);
    fd = open(self_path.data(), O_CREAT | O_EXCL | O_RDONLY, 0600);
    checks.expect(fd == -1 && errno == EEXIST, "exclusive creation cannot duplicate an existing descriptor link");
    if (fd >= 0) close(fd);
    close(stream[0]);
    close(stream[1]);
    value = 'X';
    checks.expect(write(commands[1], &value, 1) == 1 && wait_ok(TARGET), "descriptor target exits cleanly");
    close(ready[0]);
    close(commands[1]);
    if (DIRECTORY >= 0) close(DIRECTORY);
    close(ORIGINAL);
    unlink(file_path.data());
    return checks.finish();
}

auto procfs_private_policy() -> int {
    Checks checks;
    constexpr std::array<const char*, 3> NAMES{"cmdline", "maps", "images"};
    pid_t const PARENT = getpid();
    std::array<int, NAMES.size()> inherited{};
    inherited.fill(-1);
    std::array<char, 128> path{};
    for (size_t i = 0; i < NAMES.size(); ++i) {
        std::snprintf(path.data(), path.size(), "/proc/%ld/%s", static_cast<long>(PARENT), NAMES[i]);
        inherited[i] = open(path.data(), O_RDONLY);
        std::array<char, 32> output{};
        checks.expect(inherited[i] >= 0 && pread(inherited[i], output.data(), output.size(), 0) > 0, "prime private procfs view as root");
    }
    std::snprintf(path.data(), path.size(), "/proc/%ld/fd", static_cast<long>(PARENT));
    int const DIRECTORY = open(path.data(), O_RDONLY | O_DIRECTORY);
    DIR* listing = DIRECTORY >= 0 ? fdopendir(dup(DIRECTORY)) : nullptr;
    if (listing != nullptr) {
        while (readdir(listing) != nullptr) {
        }
        closedir(listing);
    }
    checks.expect(DIRECTORY >= 0 && lseek(DIRECTORY, 0, SEEK_SET) == 0, "prime FD directory EOF and rewind before credential drop");
    pid_t const CHILD = fork();
    if (CHILD == 0) {
        Checks child;
        if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        for (size_t i = 0; i < NAMES.size(); ++i) {
            std::array<char, 4> output{'s', 'a', 'f', 'e'};
            child.expect(
                pread(inherited[i], output.data(), output.size(), 0) == -1 && errno == EACCES && std::memcmp(output.data(), "safe", 4) == 0,
                "inherited private cache denies after caller privilege loss");
            child.expect(lseek(inherited[i], 0, SEEK_END) == -1 && errno == EACCES, "private cached length is not exposed by seek");
            for (bool alias : {false, true}) {
                if (alias)
                    std::snprintf(path.data(), path.size(), "/proc/%ld/task/%ld/%s", static_cast<long>(PARENT), static_cast<long>(PARENT),
                                  NAMES[i]);
                else
                    std::snprintf(path.data(), path.size(), "/proc/%ld/%s", static_cast<long>(PARENT), NAMES[i]);
                int const FD = open(path.data(), O_RDONLY);
                child.expect(FD == -1 && errno == EACCES, "foreign private procfs path and task alias deny open");
                if (FD >= 0) close(FD);
            }
            std::snprintf(path.data(), path.size(), "/proc/self/%s", NAMES[i]);
            int const SELF = open(path.data(), O_RDONLY);
            child.expect(SELF >= 0 && read(SELF, output.data(), output.size()) > 0,
                         "nondumpable nonroot can read its own private procfs view");
            if (SELF >= 0) close(SELF);
        }
        for (const char* name : {"exe", "cwd", "root"}) {
            for (bool alias : {false, true}) {
                if (alias)
                    std::snprintf(path.data(), path.size(), "/proc/%ld/task/%ld/%s", static_cast<long>(PARENT), static_cast<long>(PARENT),
                                  name);
                else
                    std::snprintf(path.data(), path.size(), "/proc/%ld/%s", static_cast<long>(PARENT), name);
                std::array<char, 4> output{'s', 'a', 'f', 'e'};
                child.expect(readlink(path.data(), output.data(), output.size()) == -1 && errno == EACCES &&
                                 std::memcmp(output.data(), "safe", 4) == 0,
                             "foreign private link denies without modifying output");
            }
            std::snprintf(path.data(), path.size(), "/proc/self/%s", name);
            std::array<char, 128> output{};
            child.expect(readlink(path.data(), output.data(), output.size()) > 0, "own private process links remain available");
        }
        std::snprintf(path.data(), path.size(), "/proc/%ld/fd", static_cast<long>(PARENT));
        int const FRESH_DIR = open(path.data(), O_RDONLY | O_DIRECTORY);
        child.expect(FRESH_DIR == -1 && errno == EACCES, "foreign FD directory denies open");
        if (FRESH_DIR >= 0) close(FRESH_DIR);
        DIR* denied = DIRECTORY >= 0 ? fdopendir(dup(DIRECTORY)) : nullptr;
        errno = 0;
        child.expect(denied != nullptr && readdir(denied) == nullptr && errno == EACCES,
                     "inherited FD directory denies dots despite cached EOF");
        if (denied != nullptr) {
            errno = 0;
            child.expect(readdir(denied) == nullptr && errno == EACCES, "repeated denied readdir preserves empty refill state");
            struct dirent entry{};
            entry.d_ino = 12345;
            struct dirent* result = &entry;
            errno = ERANGE;
            child.expect(readdir_r(denied, &entry, &result) == EACCES && result == nullptr && entry.d_ino == 12345 && errno == ERANGE,
                         "readdir_r returns denial without changing caller entry or errno");
            closedir(denied);
        }
        DIR* own = opendir("/proc/self/fd");
        child.expect(own != nullptr && readdir(own) != nullptr, "own nondumpable FD enumeration remains available");
        if (own != nullptr) closedir(own);
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "private procfs caller-loss subprocess");
    for (int fd : inherited)
        if (fd >= 0) close(fd);
    if (DIRECTORY >= 0) close(DIRECTORY);

    std::array<int, 2> ready{}, commands{};
    if (pipe(ready.data()) != 0 || pipe(commands.data()) != 0) return 1;
    pid_t const TARGET = fork();
    if (TARGET == 0) {
        close(ready[0]);
        close(commands[1]);
        if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0 || dumpability(true) != 0 || chdir("/") != 0) _exit(1);
        // Intentionally leave an own-Task-pinning file open until exit closes it.
        if (open("/proc/self/maps", O_RDONLY) < 0) _exit(1);
        char command = 'R';
        if (write(ready[1], &command, 1) != 1) _exit(1);
        while (read(commands[0], &command, 1) == 1) {
            if (command == 'X') _exit(0);
            if (command == 'C') {
                for (unsigned i = 0; i < 2000; ++i) {
                    if (chdir((i & 1U) == 0 ? "/" : "/tmp") != 0) _exit(1);
                    if ((i & 31U) == 0) {
                        const char* name = "proc-private";
                        std::array<char, 16> copied{};
                        if (process_call(ker::abi::process::procmgmt_ops::PRCTL, 15, reinterpret_cast<uint64_t>(name)) != 0 ||
                            process_call(ker::abi::process::procmgmt_ops::PRCTL, 16, reinterpret_cast<uint64_t>(copied.data())) != 0 ||
                            std::strcmp(copied.data(), name) != 0)
                            _exit(1);
                    }
                }
                if (write(ready[1], &command, 1) != 1) _exit(1);
                continue;
            }
            if (dumpability(command == 'R') != 0 || write(ready[1], &command, 1) != 1) _exit(1);
        }
        _exit(1);
    }
    close(ready[1]);
    close(commands[0]);
    char command{};
    checks.expect(read(ready[0], &command, 1) == 1 && command == 'R', "dumpable private-view target ready");
    checks.expect(setgroups(0, nullptr) == 0 && setgid(2000) == 0 && setuid(1000) == 0, "drop private-view caller identity");
    for (size_t i = 0; i < NAMES.size(); ++i) {
        std::snprintf(path.data(), path.size(), "/proc/%ld/%s", static_cast<long>(TARGET), NAMES[i]);
        inherited[i] = open(path.data(), O_RDONLY);
        std::array<char, 32> output{};
        checks.expect(inherited[i] >= 0 && pread(inherited[i], output.data(), output.size(), 0) > 0,
                      "equal-identity dumpable target view opens and primes");
    }
    for (char change : {'D', 'R'}) {
        command = change;
        checks.expect(write(commands[1], &command, 1) == 1 && read(ready[0], &command, 1) == 1,
                      "target changes dumpability with private views retained");
        for (int fd : inherited) {
            std::array<char, 4> output{'s', 'a', 'f', 'e'};
            ssize_t const RESULT = pread(fd, output.data(), output.size(), 0);
            checks.expect(change == 'D' ? RESULT == -1 && errno == EACCES && std::memcmp(output.data(), "safe", 4) == 0 : RESULT > 0,
                          "private cache follows target authority revocation and restoration");
        }
    }
    command = 'C';
    checks.expect(write(commands[1], &command, 1) == 1, "start private path and process-name publication race");
    std::snprintf(path.data(), path.size(), "/proc/%ld/cwd", static_cast<long>(TARGET));
    bool coherent = true;
    for (unsigned i = 0; i < 1000; ++i) {
        std::array<char, 128> cwd{};
        ssize_t const LENGTH = readlink(path.data(), cwd.data(), cwd.size() - 1);
        coherent &= (LENGTH == 1 && std::strcmp(cwd.data(), "/") == 0) || (LENGTH == 4 && std::strcmp(cwd.data(), "/tmp") == 0);
    }
    checks.expect(coherent && read(ready[0], &command, 1) == 1 && command == 'C',
                  "private cwd copies stay coherent across 2000 path publications");
    command = 'X';
    checks.expect(write(commands[1], &command, 1) == 1 && wait_ok(TARGET),
                  "target exits with private views and own procfs descriptor retained");
    for (int fd : inherited) {
        std::array<char, 4> output{'s', 'a', 'f', 'e'};
        checks.expect(pread(fd, output.data(), output.size(), 0) == -1 && errno == ESRCH && std::memcmp(output.data(), "safe", 4) == 0,
                      "retained private view cannot return cache after target exit");
        if (fd >= 0) close(fd);
    }
    close(ready[0]);
    close(commands[1]);
    return checks.finish();
}

struct PublicProcReadState {
    int fd{-1};
    std::atomic<unsigned> ready{};
    std::atomic<bool> start{};
};
struct PublicProcReader {
    PublicProcReadState* state{};
    uint64_t hash{};
    unsigned failures{};
};
auto read_shared_public_proc(void* opaque) -> void* {
    auto& reader = *static_cast<PublicProcReader*>(opaque);
    auto& state = *reader.state;
    state.ready.fetch_add(1, std::memory_order_release);
    while (!state.start.load(std::memory_order_acquire)) sched_yield();
    for (unsigned i = 0; i < 500; ++i) {
        std::array<char, 4096> text{};
        ssize_t const LENGTH = pread(state.fd, text.data(), text.size() - 1, 0);
        if (LENGTH <= 0 || std::strncmp(text.data(), "Name:\ttestprog\n", 15) != 0 ||
            std::strstr(text.data(), "\nUid:\t0\t0\t0\t0\n") == nullptr || lseek(state.fd, 0, SEEK_END) != LENGTH) {
            ++reader.failures;
            continue;
        }
        uint64_t hash = 1469598103934665603ULL;
        for (ssize_t byte = 0; byte < LENGTH; ++byte) hash = (hash ^ static_cast<unsigned char>(text[byte])) * 1099511628211ULL;
        if (i == 0)
            reader.hash = hash;
        else if (reader.hash != hash)
            ++reader.failures;
    }
    return nullptr;
}

auto procfs_public_policy() -> int {
    Checks checks;
    std::array<char, 128> path{};
    std::snprintf(path.data(), path.size(), "/proc/%ld/status", static_cast<long>(getpid()));
    int const SHARED = open(path.data(), O_RDONLY);
    checks.expect(SHARED >= 0, "open unprimed public process status for shared-cache race");
    pid_t const READER = fork();
    if (READER == 0) {
        Checks child;
        if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        PublicProcReadState state{.fd = SHARED};
        std::array<PublicProcReader, 4> readers{};
        std::array<pthread_t, 4> workers{};
        unsigned created = 0;
        for (size_t i = 0; i < readers.size(); ++i) {
            readers[i].state = &state;
            int const ERROR = pthread_create(&workers[i], nullptr, read_shared_public_proc, &readers[i]);
            child.expect(ERROR == 0, "create public procfs shared-cache reader");
            if (ERROR != 0) break;
            ++created;
        }
        while (state.ready.load(std::memory_order_acquire) != created) sched_yield();
        state.start.store(true, std::memory_order_release);
        bool coherent = created == readers.size();
        for (unsigned i = 0; i < created; ++i) {
            coherent &= pthread_join(workers[i], nullptr) == 0;
            coherent &= readers[i].failures == 0 && readers[i].hash == readers[0].hash;
        }
        child.expect(coherent, "2000 shared public reads preserve basename, complete credential rows and one cached snapshot");
        std::array<char, 4> output{'s', 'a', 'f', 'e'};
        std::snprintf(path.data(), path.size(), "/proc/%ld/exe", static_cast<long>(getppid()));
        child.expect(
            readlink(path.data(), output.data(), output.size()) == -1 && errno == EACCES && std::memcmp(output.data(), "safe", 4) == 0,
            "public basename does not grant access to the private executable path");
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(READER), "public procfs shared-cache subprocess");
    if (SHARED >= 0) close(SHARED);
    checks.expect(setgroups(0, nullptr) == 0 && setgid(2000) == 0 && setuid(1000) == 0, "drop public memory observer identity");
    unsigned samples = 0;
    unsigned gone = 0;
    bool sound = true;
    for (unsigned round = 0; round < 4; ++round) {
        std::array<int, 2> ready{}, commands{};
        if (pipe(ready.data()) != 0 || pipe(commands.data()) != 0) return 1;
        pid_t const TARGET = fork();
        if (TARGET == 0) {
            close(ready[0]);
            close(commands[1]);
            char signal = 'R';
            if (write(ready[1], &signal, 1) != 1 || read(commands[0], &signal, 1) != 1) _exit(1);
            for (unsigned i = 0; i < 1000; ++i) {
                void* region = mmap(nullptr, 16384, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
                if (region == MAP_FAILED) _exit(1);
                std::memset(region, 0x5A, 16384);
                if (munmap(region, 16384) != 0) _exit(1);
            }
            _exit(0);
        }
        close(ready[1]);
        close(commands[0]);
        char signal{};
        if (TARGET < 0 || read(ready[0], &signal, 1) != 1 || write(commands[1], &signal, 1) != 1) return 1;
        for (unsigned i = 0; i < 100; ++i) {
            for (const char* name : {"status", "stat", "statm", "wki_launcher", "wki_runner", "wki_remote_pid"}) {
                std::snprintf(path.data(), path.size(), "/proc/%ld/%s", static_cast<long>(TARGET), name);
                int const FD = open(path.data(), O_RDONLY);
                if (FD < 0) {
                    sound &= errno == ENOENT || errno == ESRCH;
                    ++gone;
                    continue;
                }
                std::array<char, 4096> text{};
                ssize_t const LENGTH = read(FD, text.data(), text.size() - 1);
                if (LENGTH > 0) {
                    ++samples;
                    if (std::strcmp(name, "status") == 0)
                        sound &= std::strncmp(text.data(), "Name:\ttestprog\n", 15) == 0 &&
                                 std::strstr(text.data(), "\nUid:\t1000\t1000\t1000\t1000\n") != nullptr;
                } else {
                    sound &= LENGTH == -1 && errno == ESRCH;
                    ++gone;
                }
                close(FD);
            }
            if ((i & 7U) == 0) {
                for (const char* aggregate : {"/proc/meminfo", "/proc/memacc/summary"}) {
                    int const FD = open(aggregate, O_RDONLY);
                    std::array<char, 128> text{};
                    sound &= FD >= 0 && read(FD, text.data(), text.size()) > 0;
                    if (FD >= 0) close(FD);
                }
            }
        }
        checks.expect(wait_ok(TARGET), "memory publication target exits during public procfs observation");
        close(ready[0]);
        close(commands[1]);
    }
    std::printf("credentials: public procfs memory race samples=%u retired=%u mappings=4000\n", samples, gone);
    checks.expect(sound && samples > 0, "public memory views remain safe across mapping replacement and target exit");
    return checks.finish();
}

auto journal_read_policy() -> int {
    using ker::abi::sys_log::JournalRecord;
    Checks checks;
    constexpr std::array<const char*, 3> PATHS{"/dev/journal", "/dev/journal/modules/kernel", "/dev/journal/modules/userspace"};
    constexpr char MARKER[] = "credential journal public submission";
    int const WRITER = open(PATHS[0], O_WRONLY);
    checks.expect(WRITER >= 0 && write(WRITER, MARKER, sizeof(MARKER) - 1) == sizeof(MARKER) - 1,
                  "root primes public journal writer and module device");
    if (WRITER >= 0) close(WRITER);
    // Records are immediate, but module aliases are published by a worker.
    // Establish that prerequisite before testing authorization on each alias.
    for (const char* path : PATHS) {
        struct stat state{};
        int result = -1;
        for (unsigned attempt = 0; attempt < 200; ++attempt) {
            result = stat(path, &state);
            if (result == 0 || errno != ENOENT) break;
            usleep(10000);
        }
        checks.expect(result == 0, "journal module device publication completes");
        if (result != 0) return checks.finish();
    }
    std::array<int, PATHS.size()> retained{};
    retained.fill(-1);
    for (size_t i = 0; i < PATHS.size(); ++i) {
        retained[i] = open(PATHS[i], O_RDONLY | O_NONBLOCK);
        JournalRecord record{};
        checks.expect(retained[i] >= 0 && read(retained[i], &record, sizeof(record)) == sizeof(record) &&
                          record.magic == ker::abi::sys_log::JOURNAL_RECORD_MAGIC,
                      "authorized global and module journal readers receive valid records");
    }
    struct stat persisted{}, directory{};
    checks.expect(stat("/var/log/journal", &directory) == 0 && (directory.st_mode & 07777) == 0700 && directory.st_uid == 101 &&
                      directory.st_gid == 101,
                  "journal directory has private service ownership");
    int const JOURNAL_STAT = stat("/var/log/journal/wos.journal", &persisted);
    int const JOURNAL_ERRNO = errno;
    bool const PRIVATE = JOURNAL_STAT == 0 && S_ISREG(persisted.st_mode) && (persisted.st_mode & 07777) == 0600 &&
                         persisted.st_uid == 101 && persisted.st_gid == 101 && persisted.st_nlink == 1;
    if (!PRIVATE)
        std::fprintf(stderr, "credentials: journal stat rc=%d errno=%d mode=%o uid=%u gid=%u links=%llu\n", JOURNAL_STAT, JOURNAL_ERRNO,
                     static_cast<unsigned>(persisted.st_mode), persisted.st_uid, persisted.st_gid,
                     static_cast<unsigned long long>(persisted.st_nlink));
    checks.expect(PRIVATE, "persisted journal uses a private service-owned inode");
    pid_t const CHILD = fork();
    if (CHILD == 0) {
        Checks child;
        if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        for (size_t i = 0; i < PATHS.size(); ++i) {
            for (int flags : {O_RDONLY, O_RDWR}) {
                int const FD = open(PATHS[i], flags | O_NONBLOCK);
                child.expect(FD == -1 && errno == EACCES, "fresh readable journal open requires diagnostic authority");
                if (FD >= 0) close(FD);
            }
            std::array<unsigned char, sizeof(JournalRecord)> output{};
            for (size_t count : {size_t{0}, size_t{1}, output.size()}) {
                output.fill(0xA5);
                child.expect(pread(retained[i], output.data(), count, 0) == -1 && errno == EACCES &&
                                 std::all_of(output.begin(), output.end(), [](unsigned char byte) { return byte == 0xA5; }),
                             "inherited journal read revokes without output including zero and short buffers");
            }
            struct pollfd poller{.fd = retained[i], .events = POLLIN, .revents = 0};
            child.expect(poll(&poller, 1, 0) == 1 && (poller.revents & POLLERR) != 0 && (poller.revents & POLLIN) == 0,
                         "revoked journal poll reports error without readable-record information");
            int const FD = open(PATHS[i], O_WRONLY | O_NONBLOCK);
            child.expect(FD >= 0 && write(FD, MARKER, sizeof(MARKER) - 1) == sizeof(MARKER) - 1,
                         "unprivileged write-only journal submission stays available");
            poller = {.fd = FD, .events = POLLOUT, .revents = 0};
            child.expect(FD >= 0 && poll(&poller, 1, 0) == 1 && (poller.revents & POLLOUT) != 0,
                         "public journal write readiness stays available");
            if (FD >= 0) close(FD);
        }
        int const LOG = open("/var/log/journal/wos.journal", O_RDONLY);
        child.expect(LOG == -1 && errno == EACCES, "persisted journal does not bypass device read authority");
        if (LOG >= 0) close(LOG);
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "journal denial and public writer subprocess");
    for (int fd : retained)
        if (fd >= 0) close(fd);
    int const FD = open(PATHS[2], O_RDONLY | O_NONBLOCK);
    bool found = false;
    for (unsigned attempt = 0; FD >= 0 && attempt < 4096; ++attempt) {
        JournalRecord record{};
        if (read(FD, &record, sizeof(record)) != sizeof(record)) break;
        if (record.pid == static_cast<uint64_t>(CHILD) && record.message_len == sizeof(MARKER) - 1 &&
            std::memcmp(record.message, MARKER, sizeof(MARKER) - 1) == 0) {
            found = true;
            break;
        }
    }
    checks.expect(found, "authorized reader sees the unprivileged user's submitted record");
    if (FD >= 0) close(FD);
    return checks.finish();
}

auto find_credential_core(const char* prefix, std::array<char, 256>& path, struct stat& state) -> bool {
    DIR* directory = opendir("/tmp");
    if (directory == nullptr) return false;
    bool found = false;
    while (auto* entry = readdir(directory)) {
        if (std::strncmp(entry->d_name, prefix, std::strlen(prefix)) != 0 || std::strstr(entry->d_name, "_coredump.bin") == nullptr)
            continue;
        std::snprintf(path.data(), path.size(), "/tmp/%s", entry->d_name);
        found = lstat(path.data(), &state) == 0;
        break;
    }
    closedir(directory);
    return found;
}

auto coredump_output_policy() -> int {
    Checks checks;
    for (bool enabled : {false, true}) {
        std::array<char, 16> name{};
        std::snprintf(name.data(), name.size(), "cc%ld%c", static_cast<long>(getpid()), enabled ? 'y' : 'n');
        std::array<char, 24> prefix{};
        std::snprintf(prefix.data(), prefix.size(), "%s_", name.data());
        std::array<char, 256> path{};
        struct stat state{};
        checks.expect(!find_credential_core(prefix.data(), path, state), "no preexisting credential core fixture");
        pid_t const CHILD = fork();
        if (CHILD == 0) {
            rlimit const LIMIT{RLIM_INFINITY, RLIM_INFINITY};
            if (setrlimit(RLIMIT_CORE, &LIMIT) != 0 || setrlimit(RLIMIT_FSIZE, &LIMIT) != 0 || setgroups(0, nullptr) != 0 ||
                setgid(2000) != 0 || setuid(1000) != 0 || process_call(ker::abi::process::procmgmt_ops::PRCTL, 3) != 0 ||
                process_call(ker::abi::process::procmgmt_ops::PRCTL, 15, reinterpret_cast<uint64_t>(name.data())) != 0 ||
                (enabled && dumpability(true) != 0))
                _exit(1);
            asm volatile("ud2");
            __builtin_unreachable();
        }
        int status = 0;
        pid_t waited;
        do {
            waited = waitpid(CHILD, &status, 0);
        } while (waited < 0 && errno == EINTR);
        checks.expect(CHILD > 0 && waited == CHILD && WIFSIGNALED(status) && WTERMSIG(status) == SIGILL,
                      "credential core fixture reaches an unhandled userspace fault");
        bool found = false;
        unsigned stable = 0;
        off_t previous = -1;
        for (unsigned attempt = 0; attempt < (enabled ? 300U : 100U); ++attempt) {
            found = find_credential_core(prefix.data(), path, state);
            if (found && !enabled) break;
            if (found && state.st_size > 0) {
                stable = state.st_size == previous ? stable + 1 : 0;
                previous = state.st_size;
                if (stable == 5) break;
            }
            usleep(10000);
        }
        if (!enabled) {
            checks.expect(!found, "identity-drop nondumpable fault creates no diagnostic artifact");
            continue;
        }
        checks.expect(found && stable == 5 && S_ISREG(state.st_mode) && (state.st_mode & 07777) == 0600 && state.st_uid == 0 &&
                          state.st_gid == 0 && state.st_nlink == 1,
                      "explicitly dumpable fault creates a new private administrator-owned regular file");
        if (!found || stable != 5) return 1;
        int const FD = open(path.data(), O_RDONLY);
        struct Prefix {
            uint64_t magic;
            uint32_t version;
            uint32_t header_size;
            uint64_t timestamp;
            uint64_t pid;
        } header{};
        checks.expect(FD >= 0 && read(FD, &header, sizeof(header)) == sizeof(header) && header.magic == UINT64_C(0x504d55444f43534f) &&
                          header.version == 4 && header.pid == static_cast<uint64_t>(CHILD) && header.header_size > sizeof(header),
                      "private output preserves coredump magic version and crashed process identity");
        if (FD >= 0) close(FD);
        pid_t const READER = fork();
        if (READER == 0) {
            Checks denied;
            if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
            int const OPENED = open(path.data(), O_RDONLY);
            denied.expect(OPENED == -1 && errno == EACCES, "crashed user's identity cannot read kernel diagnostic contents");
            if (OPENED >= 0) close(OPENED);
            denied.expect(chmod(path.data(), 0644) == -1 && errno == EPERM, "unprivileged observer cannot widen core permissions");
            denied.expect(chown(path.data(), 1000, 2000) == -1 && errno == EPERM, "unprivileged observer cannot take core ownership");
            int const RESULT = denied.finish();
            std::fflush(nullptr);
            _exit(RESULT);
        }
        checks.expect(wait_ok(READER), "private coredump read and metadata denials");
        struct stat after{};
        checks.expect(stat(path.data(), &after) == 0 && after.st_ino == state.st_ino && after.st_mode == state.st_mode &&
                          after.st_uid == state.st_uid && after.st_gid == state.st_gid && after.st_size == state.st_size &&
                          after.st_mtim.tv_sec == state.st_mtim.tv_sec && after.st_mtim.tv_nsec == state.st_mtim.tv_nsec &&
                          after.st_ctim.tv_sec == state.st_ctim.tv_sec && after.st_ctim.tv_nsec == state.st_ctim.tv_nsec,
                      "denied access preserves completed core inode and metadata");
        std::printf("credentials: private coredump %s bytes=%lld\n", path.data(), static_cast<long long>(after.st_size));
        checks.expect(unlink(path.data()) == 0, "administrator removes only the completed test artifact");
    }
    return checks.finish();
}

auto procfs_wki_identity_policy() -> int {
    Checks checks;
    constexpr std::array<const char*, 3> NAMES{"wki_launcher", "wki_runner", "wki_remote_pid"};
    std::array<std::array<char, 128>, NAMES.size()> expected{};
    std::array<ssize_t, NAMES.size()> lengths{};
    std::array<char, 160> path{};
    for (size_t i = 0; i < NAMES.size(); ++i) {
        std::snprintf(path.data(), path.size(), "/proc/self/%s", NAMES[i]);
        int const FD = open(path.data(), O_RDONLY);
        lengths[i] = FD >= 0 ? read(FD, expected[i].data(), expected[i].size() - 1) : -1;
        checks.expect(lengths[i] > 0 && lengths[i] <= 64 && expected[i][lengths[i] - 1] == '\n',
                      "local WKI identity has bounded line output");
        if (FD >= 0) close(FD);
    }
    checks.expect(std::strcmp(expected[0].data(), expected[1].data()) == 0 && std::strcmp(expected[2].data(), "0\n") == 0,
                  "local launcher and runner agree with zero remote PID");
    checks.expect(setgroups(0, nullptr) == 0 && setgid(2000) == 0 && setuid(1000) == 0, "drop WKI identity observer privileges");
    for (size_t i = 0; i < NAMES.size(); ++i) {
        for (unsigned alias = 0; alias < 3; ++alias) {
            if (alias == 0) std::snprintf(path.data(), path.size(), "/proc/self/%s", NAMES[i]);
            if (alias == 1) std::snprintf(path.data(), path.size(), "/proc/%ld/%s", static_cast<long>(getpid()), NAMES[i]);
            if (alias == 2) std::snprintf(path.data(), path.size(), "/proc/self/task/%ld/%s", static_cast<long>(getpid()), NAMES[i]);
            int const FD = open(path.data(), O_RDONLY);
            std::array<char, 128> output{};
            checks.expect(FD >= 0 && read(FD, output.data(), output.size() - 1) == lengths[i] && output == expected[i],
                          "public WKI identity remains consistent through PID/task aliases after credential drop");
            if (FD >= 0) close(FD);
        }
    }
    std::array<int, 2> ready{}, commands{};
    if (pipe(ready.data()) != 0 || pipe(commands.data()) != 0) return 1;
    pid_t const TARGET = fork();
    if (TARGET == 0) {
        close(ready[0]);
        close(commands[1]);
        char signal = 'R';
        if (write(ready[1], &signal, 1) != 1 || read(commands[0], &signal, 1) != 1) _exit(1);
        _exit(0);
    }
    close(ready[1]);
    close(commands[0]);
    char signal{};
    if (TARGET < 0 || read(ready[0], &signal, 1) != 1) return 1;
    std::array<int, NAMES.size()> unprimed{}, primed{};
    unprimed.fill(-1);
    primed.fill(-1);
    for (size_t i = 0; i < NAMES.size(); ++i) {
        std::snprintf(path.data(), path.size(), "/proc/%ld/%s", static_cast<long>(TARGET), NAMES[i]);
        unprimed[i] = open(path.data(), O_RDONLY);
        primed[i] = open(path.data(), O_RDONLY);
        std::array<char, 128> output{};
        checks.expect(unprimed[i] >= 0 && primed[i] >= 0 && pread(primed[i], output.data(), output.size() - 1, 0) == lengths[i] &&
                          output == expected[i],
                      "open distinct unprimed and primed target WKI identity views");
    }
    checks.expect(write(commands[1], &signal, 1) == 1 && wait_ok(TARGET), "WKI identity target exits before deferred view generation");
    for (size_t i = 0; i < NAMES.size(); ++i) {
        std::array<char, 4> sentinel{'s', 'a', 'f', 'e'};
        checks.expect(
            pread(unprimed[i], sentinel.data(), sentinel.size(), 0) == -1 && errno == ESRCH && std::memcmp(sentinel.data(), "safe", 4) == 0,
            "unprimed WKI view of retired PID fails without fabricated identity or output");
        std::array<char, 128> output{};
        checks.expect(pread(primed[i], output.data(), output.size() - 1, 0) == lengths[i] && output == expected[i],
                      "already-generated public WKI snapshot keeps its existing per-open semantics");
        if (unprimed[i] >= 0) close(unprimed[i]);
        if (primed[i] >= 0) close(primed[i]);
    }
    close(ready[0]);
    close(commands[1]);
    return checks.finish();
}

auto removal_policy(const char* base) -> int {
    Checks checks;
    char path[96];
    std::snprintf(path, sizeof(path), "%s/credentials-remove-%ld", base, static_cast<long>(getpid()));
    bool ready = mkdir(path, 0755) == 0;
    int const DIR = ready ? open(path, O_RDONLY | O_DIRECTORY) : -1;
    constexpr std::array<const char*, 6> PARENTS{"no-write", "no-search", "sticky", "group", "parent-owner", "plain"};
    constexpr std::array<mode_t, 6> MODES{0555, 0666, 01777, 0770, 01777, 0777};
    std::array<int, PARENTS.size()> parents{}, files{};
    parents.fill(-1);
    files.fill(-1);
    constexpr char DATA[] = "survive rejected removal";
    auto setup = [&](bool success, const char* operation, size_t index) {
        if (!success) std::fprintf(stderr, "credentials: removal setup %s parent=%s errno=%d\n", operation, PARENTS[index], errno);
        return success;
    };
    for (size_t i = 0; i < PARENTS.size() && ready; ++i) {
        ready = setup(mkdirat(DIR, PARENTS[i], 0777) == 0, "mkdir parent", i);
        parents[i] = ready ? openat(DIR, PARENTS[i], O_RDONLY | O_DIRECTORY) : -1;
        ready = ready && setup(parents[i] >= 0, "open parent", i);
        files[i] = ready ? openat(parents[i], "file", O_CREAT | O_EXCL | O_RDWR, 0600) : -1;
        ready = ready && setup(files[i] >= 0, "create file", i) && setup(write(files[i], DATA, sizeof(DATA)) == sizeof(DATA), "write", i) &&
                setup(mkdirat(parents[i], "empty", 0700) == 0, "mkdir empty", i) &&
                setup(mkdirat(parents[i], "full", 0700) == 0, "mkdir full", i) &&
                setup(symlinkat("../file", parents[i], "full/entry") == 0, "symlink nested", i) &&
                setup(linkat(parents[i], "file", parents[i], "alias", 0) == 0, "hardlink", i) &&
                setup(symlinkat("file", parents[i], "link") == 0, "symlink", i) &&
                setup(fchown(parents[i], i == 4 ? 1000 : 0, i == 3 ? 3000 : 0) == 0, "chown parent", i) &&
                setup(fchmod(parents[i], MODES[i]) == 0, "chmod parent", i);
    }
    // A sticky directory permits removing an owned symlink even when its referent is foreign.
    ready = ready && fchownat(parents[2], "link", 1000, 2000, AT_SYMLINK_NOFOLLOW) == 0;
    std::array<struct stat, 3> before{}, parent_before{};
    for (size_t i = 0; i < before.size() && ready; ++i)
        ready = fstat(files[i], &before[i]) == 0 && fstat(parents[i], &parent_before[i]) == 0;
    checks.expect(ready, "prepare parent search/write, sticky, group and ownership deletion fixtures");
    pid_t const CHILD = ready ? fork() : -1;
    if (CHILD == 0) {
        Checks child;
        std::array<gid_t, 1> const GROUPS{3000};
        if (setgroups(GROUPS.size(), GROUPS.data()) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        for (size_t i = 0; i < 3; ++i) {
            int const ERROR = i == 2 ? EPERM : EACCES;
            for (const char* name : {"file", "alias"})
                child.expect(unlinkat(parents[i], name, 0) == -1 && errno == ERROR,
                             "inherited parent fd cannot bypass removal permissions or canonical sticky owner");
            for (const char* name : {"empty", "full"})
                child.expect(unlinkat(parents[i], name, AT_REMOVEDIR) == -1 && errno == ERROR,
                             "directory deletion rejects before emptiness disclosure");
            struct stat state{};
            child.expect(fstat(files[i], &state) == 0 && state.st_ino == before[i].st_ino && state.st_nlink == before[i].st_nlink &&
                             state.st_size == before[i].st_size && same_times(state, before[i]),
                         "denied removals preserve target identity links size and timestamps");
            child.expect(
                fstat(parents[i], &state) == 0 && state.st_nlink == parent_before[i].st_nlink && same_times(state, parent_before[i]),
                "denied removals preserve parent links and timestamps");
        }
        child.expect(unlinkat(parents[2], "link", 0) == 0, "sticky deletion uses symlink owner without following its foreign referent");
        for (size_t i = 3; i < PARENTS.size(); ++i) {
            child.expect(unlinkat(parents[i], "file", 0) == 0 && unlinkat(parents[i], "alias", 0) == 0,
                         "parent group membership, parent ownership or ordinary write/search permits removing a private foreign file");
            child.expect(unlinkat(parents[i], "empty", AT_REMOVEDIR) == 0, "authorized empty directory removal succeeds");
            child.expect(unlinkat(parents[i], "full", AT_REMOVEDIR) == -1 && errno == ENOTEMPTY,
                         "authorized nonempty directory keeps ENOTEMPTY");
            std::array<char, sizeof(DATA)> output{};
            child.expect(
                pread(files[i], output.data(), output.size(), 0) == output.size() && std::memcmp(output.data(), DATA, sizeof(DATA)) == 0,
                "unlinked inode remains readable through its retained descriptor");
        }
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "parent and sticky removal authorization subprocess");
    if (ready) {
        // Changing the canonical owner after hardlink creation must authorize the alias.
        checks.expect(fchown(files[2], 1000, 2000) == 0, "change canonical sticky target owner");
        pid_t const OWNER = fork();
        if (OWNER == 0) {
            Checks child;
            if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
            child.expect(unlinkat(parents[2], "alias", 0) == 0, "sticky hardlink uses current canonical owner");
            int const RESULT = child.finish();
            std::fflush(nullptr);
            _exit(RESULT);
        }
        checks.expect(wait_ok(OWNER), "canonical sticky owner removal subprocess");
    }
    for (size_t i = 0; i < PARENTS.size(); ++i) {
        if (files[i] >= 0) close(files[i]);
        if (parents[i] < 0) continue;
        fchmod(parents[i], 0777);
        for (const char* name : {"file", "alias", "link", "full/entry"}) unlinkat(parents[i], name, 0);
        for (const char* name : {"empty", "full"}) unlinkat(parents[i], name, AT_REMOVEDIR);
        close(parents[i]);
        checks.expect(unlinkat(DIR, PARENTS[i], AT_REMOVEDIR) == 0, "remove exact deletion fixture parent");
    }
    if (DIR >= 0) close(DIR);
    checks.expect(rmdir(path) == 0, "remove deletion fixture root");
    return checks.finish();
}

auto fat_removal_policy() -> int {
    Checks checks;
    char path[96];
    constexpr const char* DIRECTORY = "/boot/EFI";
    std::snprintf(path, sizeof(path), "/boot/credentials-remove-%ld.txt", static_cast<long>(getpid()));
    int const FILE = open(path, O_CREAT | O_EXCL | O_RDWR, 0644);
    constexpr char DATA[] = "FAT removal sentinel";
    struct stat before{}, directory_before{};
    bool const READY = FILE >= 0 && write(FILE, DATA, sizeof(DATA)) == sizeof(DATA) && fstat(FILE, &before) == 0 &&
                       stat(DIRECTORY, &directory_before) == 0 && S_ISDIR(directory_before.st_mode);
    checks.expect(READY, "prepare disposable FAT deletion fixture");
    pid_t const CHILD = READY ? fork() : -1;
    if (CHILD == 0) {
        Checks child;
        if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        child.expect(unlink(path) == -1 && errno == EACCES, "fixed root-owned FAT parent denies unlink");
        child.expect(rmdir(DIRECTORY) == -1 && errno == EACCES, "fixed root-owned FAT parent denies rmdir");
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "FAT removal authorization subprocess");
    struct stat after{};
    std::array<char, sizeof(DATA)> output{};
    checks.expect(READY && stat(path, &after) == 0 && after.st_size == before.st_size && same_times(before, after) &&
                      pread(FILE, output.data(), output.size(), 0) == output.size() && std::memcmp(output.data(), DATA, sizeof(DATA)) == 0,
                  "denied FAT removal preserves target data and metadata");
    if (FILE >= 0) close(FILE);
    checks.expect(stat(DIRECTORY, &after) == 0 && after.st_mode == directory_before.st_mode && same_times(after, directory_before),
                  "denied FAT rmdir preserves existing boot directory");
    checks.expect(unlink(path) == 0, "root removes exact FAT deletion file");
    return checks.finish();
}

auto rename_policy(const char* base) -> int {
    Checks checks;
    char path[112];
    std::snprintf(path, sizeof(path), "%s/credentials-rename-%ld", base, static_cast<long>(getpid()));
    int const DIR = mkdir(path, 0755) == 0 ? open(path, O_RDONLY | O_DIRECTORY) : -1;
    constexpr std::array<const char*, 4> NAMES{"source", "destination", "denied", "sticky"};
    constexpr std::array<mode_t, 4> MODES{0777, 0777, 0555, 01777};
    std::array<int, 4> parents{-1, -1, -1, -1};
    bool ready = DIR >= 0;
    for (size_t i = 0; i < parents.size() && ready; ++i) {
        ready = mkdirat(DIR, NAMES[i], 0755) == 0;
        parents[i] = ready ? openat(DIR, NAMES[i], O_RDONLY | O_DIRECTORY) : -1;
        ready = parents[i] >= 0 && fchmod(parents[i], MODES[i]) == 0;
    }
    auto make = [](int dir, const char* name, uid_t uid, mode_t mode) {
        int fd = openat(dir, name, O_CREAT | O_EXCL | O_RDWR, 0600);
        if (fd < 0) return false;
        bool ok = write(fd, "payload", 7) == 7 && fchown(fd, uid, 2000) == 0 && fchmod(fd, mode) == 0;
        close(fd);
        return ok;
    };
    ready = ready && make(parents[0], "owned", 1000, 0000) && make(parents[0], "foreign", 4000, 0000) &&
            make(parents[0], "replacement", 1000, 0600) && make(parents[1], "victim", 4000, 0600) &&
            make(parents[2], "protected", 1000, 0600) && make(parents[3], "protected", 4000, 0600);
    ready = ready && linkat(parents[0], "owned", parents[1], "same", 0) == 0 &&
            linkat(parents[1], "victim", parents[1], "victim-alias", 0) == 0;
    int const VICTIM = ready ? openat(parents[1], "victim", O_RDONLY) : -1;
    ready = ready && VICTIM >= 0 && mkdirat(parents[0], "readonly", 0555) == 0 && mkdirat(parents[0], "tree", 0777) == 0 &&
            mkdirat(parents[0], "tree/child", 0777) == 0 && mkdirat(parents[1], "empty", 0777) == 0 &&
            fchmodat(parents[0], "tree", 0777, 0) == 0 && fchmodat(parents[0], "tree/child", 0777, 0) == 0 &&
            fchmodat(parents[1], "empty", 0777, 0) == 0 && symlinkat("owned", parents[0], "link") == 0 &&
            symlinkat("missing", parents[1], "dangling") == 0;
    ready = ready && renameat(DIR, "source", DIR, "moved-source") == 0 && mkdirat(DIR, "source", 0755) == 0 &&
            renameat(DIR, "destination", DIR, "moved-destination") == 0 && mkdirat(DIR, "destination", 0755) == 0;
    std::array<struct stat, 2> before{};
    for (size_t i = 0; i < before.size() && ready; ++i) ready = fstat(parents[i + 2], &before[i]) == 0;
    checks.expect(ready, "prepare retained rename parents, sticky victims and canonical aliases");
    pid_t const CHILD = ready ? fork() : -1;
    if (CHILD == 0) {
        Checks child;
        if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        child.expect(renameat(parents[2], "protected", parents[1], "bad") == -1 && errno == EACCES,
                     "rename requires source-parent write/search");
        child.expect(renameat(parents[0], "owned", parents[2], "bad") == -1 && errno == EACCES,
                     "rename requires destination-parent write/search");
        child.expect(renameat(parents[3], "protected", parents[1], "bad") == -1 && errno == EPERM,
                     "rename sticky source uses source inode ownership");
        child.expect(renameat(parents[0], "owned", parents[3], "protected") == -1 && errno == EPERM,
                     "rename sticky replacement uses displaced inode ownership");
        child.expect(renameat(parents[0], "readonly", parents[1], "bad") == -1 && errno == EACCES,
                     "cross-parent directory rename needs permission to update parent reference");
        child.expect(renameat(parents[0], "tree", parents[0], "tree/child/cycle") == -1 && errno == EINVAL,
                     "rename rejects directory ancestry cycle");
        child.expect(renameat(parents[0], "owned", parents[1], "empty") == -1 && errno == EISDIR,
                     "regular rename cannot replace directory");
        child.expect(renameat(parents[0], "tree", parents[1], "victim") == -1 && errno == ENOTDIR,
                     "directory rename cannot replace regular inode");
        child.expect(renameat(parents[1], "empty", parents[0], "tree") == -1 && errno == ENOTEMPTY,
                     "directory rename cannot replace nonempty directory");
        child.expect(renameat(parents[0], "owned/", parents[1], "bad") == -1 && errno == ENOTDIR,
                     "rename source retains directory requirement");
        child.expect(renameat(parents[0], "link/", parents[1], "bad") == -1 && errno == ENOTDIR,
                     "rename does not follow directory-required final symlink");
        child.expect(renameat(parents[0], "owned", parents[1], "missing/") == -1 && errno == ENOENT,
                     "rename missing directory-required destination stays absent");
        child.expect(renameat(parents[0], "owned", parents[1], "dangling/") == -1 && errno == ENOTDIR,
                     "rename directory-required destination uses final symlink entry");
        child.expect(renameat(parents[0], "tree/.", parents[1], "bad") == -1 && errno == EINVAL &&
                         renameat(parents[0], "owned", parents[0], "tree/child/..") == -1 && errno == EINVAL,
                     "rename rejects final dot components before canonicalization");
        struct stat original{}, alias{}, after{};
        child.expect(fstatat(parents[0], "owned", &original, 0) == 0 && fstatat(parents[1], "same", &alias, 0) == 0 &&
                         original.st_nlink == 2 && alias.st_nlink == 2,
                     "warm both canonical rename aliases");
        for (unsigned i = 0; i < 3; ++i)
            child.expect(renameat(parents[0], "owned", parents[1], "same") == 0 && fstatat(parents[0], "owned", &after, 0) == 0 &&
                             after.st_ino == original.st_ino && after.st_nlink == 2 && same_times(after, original) &&
                             fstatat(parents[1], "same", &alias, 0) == 0 && alias.st_ino == original.st_ino && alias.st_nlink == 2 &&
                             same_times(alias, original),
                         "same-canonical rename preserves both cached names and inode timestamps");
        child.expect(renameat(parents[0], "foreign", parents[1], "foreign") == 0 && fstatat(parents[1], "foreign", &after, 0) == 0 &&
                         after.st_uid == 4000 && (after.st_mode & 07777) == 0,
                     "writable parents permit foreign mode-zero rename without changing identity");
        child.expect(renameat(parents[0], "replacement", parents[1], "victim") == 0 && fstat(VICTIM, &after) == 0 && after.st_nlink == 1 &&
                         fstatat(parents[1], "victim-alias", &alias, 0) == 0 && alias.st_nlink == 1 &&
                         fstatat(parents[1], "victim", &after, 0) == 0 && after.st_uid == 1000,
                     "replacement preserves displaced inode lifetime and invalidates alias link counts");
        char data[8]{};
        child.expect(pread(VICTIM, data, 7, 0) == 7 && std::memcmp(data, "payload", 7) == 0,
                     "replaced open inode retains its original data");
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "rename credential subprocess");
    if (ready) {
        struct stat st{};
        for (size_t i = 0; i < before.size(); ++i)
            checks.expect(fstat(parents[i + 2], &st) == 0 && st.st_ino == before[i].st_ino && st.st_nlink == before[i].st_nlink &&
                              same_times(st, before[i]) && fstatat(parents[i + 2], "protected", &st, 0) == 0,
                          "denied rename preserves protected parent metadata and source/victim");
        checks.expect(fstatat(DIR, "source/owned", &st, 0) == -1 && errno == ENOENT && fstatat(DIR, "destination/victim", &st, 0) == -1 &&
                          errno == ENOENT,
                      "rename through retained descriptors leaves reused parent names untouched");
        checks.expect(renameat(parents[0], "tree/child", parents[1], "empty") == 0 && fstatat(parents[1], "empty/..", &st, 0) == 0,
                      "cross-parent directory replacement updates parent reference");
        struct stat parent{};
        checks.expect(fstat(parents[1], &parent) == 0 && st.st_ino == parent.st_ino, "moved directory resolves its new parent");
    }
    if (VICTIM >= 0) close(VICTIM);
    for (size_t i = 0; i < parents.size(); ++i) {
        if (parents[i] < 0) continue;
        fchmod(parents[i], 0777);
        for (const char* name :
             {"owned", "foreign", "replacement", "victim", "victim-alias", "same", "protected", "link", "dangling", "bad"})
            unlinkat(parents[i], name, 0);
        for (const char* name : {"tree/child", "tree", "readonly", "empty"}) unlinkat(parents[i], name, AT_REMOVEDIR);
        close(parents[i]);
        unlinkat(DIR, NAMES[i], AT_REMOVEDIR);
    }
    if (DIR >= 0) {
        unlinkat(DIR, "moved-source", AT_REMOVEDIR);
        unlinkat(DIR, "moved-destination", AT_REMOVEDIR);
        close(DIR);
    }
    checks.expect(rmdir(path) == 0, "remove rename credential fixtures");
    return checks.finish();
}

auto fat_rename_policy() -> int {
    Checks checks;
    std::array<char, 128> source{}, destination{};
    std::snprintf(source.data(), source.size(), "/boot/cred_fat_rename_%ld_source", static_cast<long>(getpid()));
    std::snprintf(destination.data(), destination.size(), "/boot/cred_fat_rename_%ld_destination", static_cast<long>(getpid()));
    int const SOURCE = open(source.data(), O_CREAT | O_EXCL | O_RDWR, 0600);
    int const DISPLACED = open(destination.data(), O_CREAT | O_EXCL | O_RDWR, 0600);
    checks.expect(SOURCE >= 0 && DISPLACED >= 0, "create FAT rename fixture");
    if (SOURCE < 0 || DISPLACED < 0) {
        if (SOURCE >= 0) {
            close(SOURCE);
            unlink(source.data());
        }
        if (DISPLACED >= 0) {
            close(DISPLACED);
            unlink(destination.data());
        }
        return checks.finish();
    }
    checks.expect(write(SOURCE, "source-data", 11) == 11 && write(DISPLACED, "destination", 11) == 11, "initialize FAT rename data");
    int const SOURCE_READER = open(source.data(), O_RDONLY);
    int const DISPLACED_READER = open(destination.data(), O_RDONLY);
    auto contents = [&](int fd, const char* expected, unsigned links) {
        std::array<char, 64> bytes{};
        size_t const LENGTH = std::strlen(expected);
        struct stat st{};
        checks.expect(fstat(fd, &st) == 0 && st.st_size == static_cast<off_t>(LENGTH) && st.st_nlink == links,
                      "FAT retained descriptor size and link count");
        checks.expect(
            pread(fd, bytes.data(), bytes.size(), 0) == static_cast<ssize_t>(LENGTH) && std::memcmp(bytes.data(), expected, LENGTH) == 0,
            "FAT retained descriptor content");
    };
    contents(SOURCE_READER, "source-data", 1);
    contents(DISPLACED_READER, "destination", 1);
    struct stat before{}, observed{};
    checks.expect(fstat(SOURCE, &before) == 0, "capture FAT source identity");
    pid_t const CHILD = fork();
    if (CHILD == 0) {
        Checks denied;
        denied.expect(setgroups(0, nullptr) == 0 && setgid(2000) == 0 && setuid(1000) == 0, "drop FAT rename caller identity");
        denied.expect(rename(source.data(), destination.data()) == -1 && errno == EACCES, "nonroot FAT replacement denied");
        denied.expect(rename(source.data(), source.data()) == -1 && errno == EACCES, "nonroot FAT same-entry rename denied");
        denied.expect(stat(source.data(), &observed) == 0 && observed.st_ino == before.st_ino && observed.st_size == before.st_size,
                      "denied FAT rename preserves source metadata");
        int const RESULT = denied.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "FAT rename denial subprocess");
    checks.expect(rename(source.data(), source.data()) == 0 && stat(source.data(), &observed) == 0 && observed.st_ino == before.st_ino,
                  "FAT same-entry rename preserves warm pathname");
    checks.expect(rename(source.data(), destination.data()) == 0, "FAT replacement with retained descriptors");
    checks.expect(stat(source.data(), &observed) == -1 && errno == ENOENT, "FAT old name removed");
    checks.expect(stat(destination.data(), &observed) == 0 && observed.st_ino == before.st_ino, "FAT destination retains source identity");
    contents(SOURCE_READER, "source-data", 1);
    contents(DISPLACED_READER, "destination", 0);
    int const REUSED = open(source.data(), O_CREAT | O_EXCL | O_RDWR, 0600);
    checks.expect(REUSED >= 0 && write(REUSED, "reused", 6) == 6, "reuse old FAT source name");
    checks.expect(pwrite(SOURCE, "XY", 2, 1) == 2, "write through renamed FAT descriptor");
    contents(SOURCE_READER, "sXYrce-data", 1);
    contents(REUSED, "reused", 1);
    checks.expect(stat(destination.data(), &observed) == 0 && observed.st_size == 11, "FAT interior write preserves pathname size");
    checks.expect(pwrite(SOURCE, "source-extended", 15, 0) == 15, "extend through historical FAT descriptor");
    checks.expect(stat(destination.data(), &observed) == 0 && observed.st_size == 15, "FAT new-name cache observes retained extension");
    contents(SOURCE_READER, "source-extended", 1);
    checks.expect(pwrite(DISPLACED, "orphan-payload", 14, 0) == 14, "write retained displaced FAT file");
    contents(DISPLACED_READER, "orphan-payload", 0);
    checks.expect(stat(destination.data(), &observed) == 0 && observed.st_size == 15 && observed.st_ino == before.st_ino,
                  "displaced FAT write preserves replacement SFN");
    contents(SOURCE_READER, "source-extended", 1);
    checks.expect(close(DISPLACED) == 0, "close first displaced FAT descriptor");
    contents(DISPLACED_READER, "orphan-payload", 0);
    checks.expect(close(DISPLACED_READER) == 0, "reclaim FAT replacement after final close");
    checks.expect(unlink(destination.data()) == 0, "unlink retained FAT source");
    contents(SOURCE_READER, "source-extended", 0);
    checks.expect(pwrite(SOURCE, "unlinked-source!", 16, 0) == 16, "write retained unlinked FAT source");
    contents(SOURCE_READER, "unlinked-source!", 0);
    checks.expect(stat(destination.data(), &observed) == -1 && errno == ENOENT, "unlinked FAT write cannot resurrect pathname");
    contents(REUSED, "reused", 1);
    checks.expect(close(SOURCE) == 0 && close(SOURCE_READER) == 0, "reclaim FAT source after final close");
    if (REUSED >= 0) checks.expect(close(REUSED) == 0, "close reused FAT file");
    checks.expect(unlink(source.data()) == 0, "remove exact FAT rename fixture");
    return checks.finish();
}

auto hardlink_policy(const char* base) -> int {
    Checks checks;
    char path[112];
    std::snprintf(path, sizeof(path), "%s/credentials-hardlink-%ld", base, static_cast<long>(getpid()));
    int const DIR = mkdir(path, 0755) == 0 ? open(path, O_RDONLY | O_DIRECTORY) : -1;
    constexpr std::array<const char*, 5> PARENTS{"source", "destination", "no-write", "no-search", "hidden"};
    constexpr std::array<mode_t, 5> PARENT_MODES{01777, 03777, 0555, 0666, 0666};
    std::array<int, 5> parents{-1, -1, -1, -1, -1};
    bool ready = DIR >= 0;
    for (size_t i = 0; i < parents.size() && ready; ++i) {
        ready = mkdirat(DIR, PARENTS[i], 0755) == 0;
        parents[i] = ready ? openat(DIR, PARENTS[i], O_RDONLY | O_DIRECTORY) : -1;
        ready = parents[i] >= 0;
    }
    constexpr std::array<const char*, 6> SOURCES{"read-only", "setuid", "setgid", "setgid-no-group-exec", "safe", "owned"};
    constexpr std::array<mode_t, 6> MODES{0444, 04777, 02777, 02061, 0060, 0000};
    constexpr std::array<uid_t, 6> USERS{4000, 4000, 4000, 4000, 4000, 1000};
    constexpr std::array<gid_t, 6> GROUPS{3000, 3000, 3000, 3000, 3000, 2000};
    std::array<int, 6> sources{-1, -1, -1, -1, -1, -1};
    std::array<struct stat, 4> protected_before{};
    for (size_t i = 0; i < sources.size() && ready; ++i) {
        sources[i] = openat(parents[0], SOURCES[i], O_CREAT | O_EXCL | O_RDWR, 0600);
        ready = sources[i] >= 0 && write(sources[i], "payload", 7) == 7 && fchown(sources[i], USERS[i], GROUPS[i]) == 0 &&
                fchmod(sources[i], MODES[i]) == 0;
        if (i < 4) ready = ready && fstat(sources[i], &protected_before[i]) == 0;
    }
    ready = ready && symlinkat("owned", parents[0], "foreign-link") == 0;
    for (size_t i = 1; i < 4 && ready; ++i) ready = symlinkat("absent-target", parents[i], "exists") == 0;
    int const HIDDEN = ready ? openat(parents[4], "owned", O_CREAT | O_EXCL | O_RDWR, 0600) : -1;
    ready = ready && HIDDEN >= 0 && fchown(HIDDEN, 1000, 2000) == 0;
    if (HIDDEN >= 0) close(HIDDEN);
    for (size_t i = 0; i < parents.size() && ready; ++i)
        ready = fchown(parents[i], 0, i == 1 ? 4000 : 0) == 0 && fchmod(parents[i], PARENT_MODES[i]) == 0;
    ready = ready && renameat(DIR, "source", DIR, "moved-source") == 0 && mkdirat(DIR, "source", 0755) == 0 &&
            renameat(DIR, "destination", DIR, "moved-destination") == 0 && mkdirat(DIR, "destination", 0755) == 0;
    std::array<struct stat, 2> parent_before{};
    for (size_t i = 0; i < 2 && ready; ++i) ready = fstat(parents[i + 2], &parent_before[i]) == 0;
    checks.expect(ready, "prepare protected and safe hardlink sources with retained renamed parents");
    pid_t const CHILD = ready ? fork() : -1;
    if (CHILD == 0) {
        Checks child;
        std::array<gid_t, 1> const MEMBER{3000};
        if (setgroups(MEMBER.size(), MEMBER.data()) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        for (size_t i = 2; i < 4; ++i) {
            child.expect(linkat(parents[0], "owned", parents[i], "new", 0) == -1 && errno == EACCES,
                         "hardlink requires destination-parent write and search");
            child.expect(linkat(parents[0], "owned", parents[i], "exists", 0) == -1 && errno == EACCES,
                         "hardlink collision cannot bypass destination-parent authority");
        }
        child.expect(linkat(parents[4], "owned", parents[1], "hidden-link", 0) == -1 && errno == EACCES,
                     "hardlink requires source-parent search even for an owned inode");
        for (const char* source : {"read-only", "setuid", "setgid", "setgid-no-group-exec", "foreign-link"})
            child.expect(linkat(parents[0], source, parents[1], source, 0) == -1 && errno == EPERM,
                         "hardlink rejects protected foreign content, either set-ID bit and symlink inodes");
        struct stat st{};
        child.expect(linkat(parents[0], "owned", parents[1], "owned", 0) == 0 && fstatat(parents[1], "owned", &st, 0) == 0 &&
                         st.st_uid == 1000 && st.st_gid == 2000 && (st.st_mode & 07777) == 0 && st.st_size == 7 && st.st_nlink == 2,
                     "owned mode-zero source can be linked without adopting destination SGID group or changing data");
        child.expect(linkat(parents[0], "safe", parents[1], "safe", 0) == 0 && fstatat(parents[1], "safe", &st, 0) == 0 &&
                         st.st_uid == 4000 && st.st_gid == 3000 && (st.st_mode & 07777) == 0060 && st.st_nlink == 2,
                     "supplementary-group read/write permits safe foreign regular hardlink with canonical identity");
        child.expect(unlinkat(parents[1], "safe", 0) == -1 && errno == EPERM,
                     "foreign hardlink keeps foreign ownership for sticky-directory removal");
        child.expect(linkat(parents[0], "owned", parents[1], "removable", 0) == 0 && unlinkat(parents[1], "removable", 0) == 0,
                     "owner can remove their hardlink in a root-owned sticky directory");
        child.expect(fstatat(parents[1], "owned", &st, 0) == 0 && st.st_nlink == 2,
                     "warm existing hardlink alias before a third name is created");
        child.expect(linkat(parents[0], "foreign-link", parents[1], "followed", AT_SYMLINK_FOLLOW) == 0 &&
                         fstatat(parents[1], "followed", &st, 0) == 0 && st.st_uid == 1000 && S_ISREG(st.st_mode),
                     "explicit source symlink following authorizes the actual owned target");
        child.expect(fstat(sources[5], &st) == 0 && st.st_nlink == 3 && fstatat(parents[1], "owned", &st, 0) == 0 && st.st_nlink == 3,
                     "third hardlink invalidates old alias metadata and keeps retained FD count coherent");
        child.expect(symlinkat("owned", parents[0], "user-symlink") == 0 &&
                         linkat(parents[0], "user-symlink", parents[1], "user-symlink", 0) == 0 &&
                         fstatat(parents[1], "user-symlink", &st, AT_SYMLINK_NOFOLLOW) == 0 && st.st_uid == 1000 && S_ISLNK(st.st_mode) &&
                         st.st_nlink == 2,
                     "owned source symlink is hardlinked as its own canonical inode by default");
        for (bool absolute : {false, true}) {
            char missing[176], existing[176];
            std::snprintf(missing, sizeof(missing), absolute ? "%s/moved-destination/trailing/" : "trailing/", path);
            std::snprintf(existing, sizeof(existing), absolute ? "%s/moved-destination/exists/" : "exists/", path);
            child.expect(linkat(parents[0], "owned", parents[1], missing, 0) == -1 && errno == ENOENT,
                         "hardlink missing directory-required destination stays absent");
            child.expect(linkat(parents[0], "owned", parents[1], existing, 0) == -1 && errno == EEXIST,
                         "hardlink trailing destination collision preserves final symlink instead of following it");
        }
        for (const char* source : {"owned/", "foreign-link/"})
            child.expect(linkat(parents[0], source, parents[1], "source-trailing", 0) == -1 && errno == ENOTDIR,
                         "hardlink source trailing slash requires an actual directory");
        child.expect(linkat(DIR, "moved-source", parents[1], "directory", 0) == -1 && errno == EPERM, "hardlink cannot link a directory");
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "hardlink credential subprocess");
    if (ready) {
        struct stat st{}, alias{};
        for (size_t i = 0; i < 4; ++i)
            checks.expect(fstat(sources[i], &st) == 0 && st.st_ino == protected_before[i].st_ino &&
                              st.st_nlink == protected_before[i].st_nlink && st.st_uid == protected_before[i].st_uid &&
                              st.st_gid == protected_before[i].st_gid && st.st_mode == protected_before[i].st_mode &&
                              same_times(st, protected_before[i]) && fstatat(parents[1], SOURCES[i], &alias, AT_SYMLINK_NOFOLLOW) == -1 &&
                              errno == ENOENT,
                          "denied hardlink leaves protected source metadata and destination namespace unchanged");
        for (size_t i = 0; i < 2; ++i)
            checks.expect(fstat(parents[i + 2], &st) == 0 && st.st_nlink == parent_before[i].st_nlink && same_times(st, parent_before[i]) &&
                              fstatat(parents[i + 2], "new", &alias, AT_SYMLINK_NOFOLLOW) == -1 && errno == ENOENT,
                          "denied hardlink leaves destination-parent metadata unchanged");
        checks.expect(fstat(sources[5], &st) == 0 && fstatat(parents[1], "owned", &alias, 0) == 0 && st.st_ino == alias.st_ino &&
                          st.st_nlink == 3 && alias.st_nlink == 3 && st.st_uid == 1000 && st.st_gid == 2000 &&
                          fstatat(DIR, "destination/owned", &alias, 0) == -1 && errno == ENOENT &&
                          fstatat(DIR, "source/owned", &alias, 0) == -1 && errno == ENOENT,
                      "hardlinks use retained original source and destination across rename and name reuse");
        for (const char* name : {"hidden-link", "foreign-link", "trailing", "absent-target", "source-trailing", "directory"})
            checks.expect(fstatat(parents[1], name, &st, AT_SYMLINK_NOFOLLOW) == -1 && errno == ENOENT,
                          "failed hardlink leaves no unexpected destination or dangling target");
        char payload[8]{};
        checks.expect(pread(sources[5], payload, 7, 0) == 7 && std::memcmp(payload, "payload", 7) == 0,
                      "hardlink creation preserves original file data");
    }
    for (int fd : sources)
        if (fd >= 0) close(fd);
    for (size_t i = 0; i < parents.size(); ++i) {
        if (parents[i] < 0) continue;
        fchmod(parents[i], 0777);
        for (const char* name :
             {"read-only", "setuid", "setgid", "setgid-no-group-exec", "safe", "owned", "foreign-link", "user-symlink", "exists", "new",
              "removable", "followed", "hidden-link", "trailing", "absent-target", "source-trailing", "directory"})
            unlinkat(parents[i], name, 0);
        close(parents[i]);
        unlinkat(DIR, PARENTS[i], AT_REMOVEDIR);
    }
    if (DIR >= 0) {
        unlinkat(DIR, "moved-source", AT_REMOVEDIR);
        unlinkat(DIR, "moved-destination", AT_REMOVEDIR);
        close(DIR);
    }
    checks.expect(rmdir(path) == 0, "remove hardlink credential fixtures");
    return checks.finish();
}

auto symlink_creation_policy(const char* base) -> int {
    Checks checks;
    char path[112];
    std::snprintf(path, sizeof(path), "%s/credentials-symlink-%ld", base, static_cast<long>(getpid()));
    int const DIR = mkdir(path, 0755) == 0 ? open(path, O_RDONLY | O_DIRECTORY) : -1;
    constexpr std::array<const char*, 5> NAMES{"no-write", "no-search", "group", "inherit", "ordinary"};
    constexpr std::array<mode_t, 5> MODES{0555, 0666, 0770, 03777, 01777};
    constexpr std::array<gid_t, 5> GROUPS{0, 0, 3000, 4000, 0};
    constexpr const char* TARGET = "../missing/../literal";
    std::array<int, 5> parents{-1, -1, -1, -1, -1};
    std::array<struct stat, 2> before{};
    bool ready = DIR >= 0;
    for (size_t i = 0; i < NAMES.size() && ready; ++i) {
        ready = mkdirat(DIR, NAMES[i], 0755) == 0;
        parents[i] = ready ? openat(DIR, NAMES[i], O_RDONLY | O_DIRECTORY) : -1;
        ready = parents[i] >= 0 && symlinkat("original", parents[i], "exists") == 0;
        ready = ready && fchown(parents[i], 0, GROUPS[i]) == 0 && fchmod(parents[i], MODES[i]) == 0;
        if (i < 2) ready = ready && fstat(parents[i], &before[i]) == 0;
    }
    ready = ready && renameat(DIR, "ordinary", DIR, "moved") == 0 && mkdirat(DIR, "ordinary", 0755) == 0;
    checks.expect(ready, "prepare protected, group, setgid/sticky and renamed symlink parents");
    pid_t const CHILD = ready ? fork() : -1;
    if (CHILD == 0) {
        Checks child;
        std::array<gid_t, 1> const MEMBER{3000};
        if (setgroups(MEMBER.size(), MEMBER.data()) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        umask(0777);
        for (size_t i = 0; i < 2; ++i) {
            child.expect(symlinkat(TARGET, parents[i], "new") == -1 && errno == EACCES,
                         "symlink requires parent write and search permissions");
            child.expect(symlinkat(TARGET, parents[i], "exists") == -1 && errno == EACCES,
                         "symlink collision does not bypass parent authorization");
        }
        for (size_t i = 0; i < 2; ++i) {
            child.expect(symlinkat(TARGET, parents[i], "new/") == -1 && errno == EACCES && symlinkat(TARGET, parents[i], "exists/") == -1 &&
                             errno == EACCES,
                         "trailing symlink destinations still require parent write/search authority");
        }
        for (bool absolute : {false, true}) {
            char missing[160], existing[160];
            std::snprintf(missing, sizeof(missing), absolute ? "%s/moved/trailing/" : "trailing/", path);
            std::snprintf(existing, sizeof(existing), absolute ? "%s/moved/exists/" : "exists/", path);
            child.expect(symlinkat(TARGET, parents[4], missing) == -1 && errno == ENOENT,
                         "symlink missing destination with trailing slash fails without creating a link");
            child.expect(symlinkat(TARGET, parents[4], existing) == -1 && errno == (base[0] == '\0' ? EEXIST : EPERM),
                         "symlink trailing final collision preserves existing backend errno without following target");
        }
        struct stat st{};
        child.expect(fstatat(parents[4], "original", &st, AT_SYMLINK_NOFOLLOW) == -1 && errno == ENOENT &&
                         fstatat(parents[4], "trailing", &st, AT_SYMLINK_NOFOLLOW) == -1 && errno == ENOENT,
                     "trailing symlink creation leaves both requested missing entry and dangling target absent");
        for (size_t i = 2; i < parents.size(); ++i) {
            child.expect(symlinkat(TARGET, parents[i], "owned") == 0 && fstatat(parents[i], "owned", &st, AT_SYMLINK_NOFOLLOW) == 0 &&
                             S_ISLNK(st.st_mode) && st.st_uid == 1000 && st.st_gid == (i == 3 ? 4000U : 2000U) &&
                             (st.st_mode & 07777) == 0777,
                         "symlink uses creator IDs, parent group inheritance and fixed mode despite umask");
            char target[64]{};
            child.expect(readlinkat(parents[i], "owned", target, sizeof(target)) == static_cast<ssize_t>(std::strlen(TARGET)) &&
                             std::strcmp(target, TARGET) == 0,
                         "symlink stores literal dangling target without resolving it");
        }
        child.expect(symlinkat("discarded", parents[3], "removable") == 0 && unlinkat(parents[3], "removable", 0) == 0,
                     "creator can remove their own link from root-owned sticky parent");
        child.expect(symlinkat("replacement", parents[4], "exists") == -1, "symlink creation preserves existing final link");
        child.expect(symlinkat(TARGET, parents[4], "absent/deep") == -1 && errno == ENOENT,
                     "symlink does not create missing intermediate directories");
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "symlink creation credential subprocess");
    if (ready) {
        struct stat st{};
        for (size_t i = 0; i < 2; ++i) {
            checks.expect(fstat(parents[i], &st) == 0 && st.st_ino == before[i].st_ino && st.st_nlink == before[i].st_nlink &&
                              st.st_mode == before[i].st_mode && same_times(st, before[i]) &&
                              fstatat(parents[i], "new", &st, AT_SYMLINK_NOFOLLOW) == -1 && errno == ENOENT,
                          "denied symlink leaves parent metadata and namespace unchanged");
        }
        for (int parent : parents) {
            char target[32]{};
            checks.expect(readlinkat(parent, "exists", target, sizeof(target)) == 8 && std::strcmp(target, "original") == 0,
                          "existing symlink target remains unchanged after denied or colliding creation");
        }
        checks.expect(fstatat(DIR, "ordinary/owned", &st, AT_SYMLINK_NOFOLLOW) == -1 && errno == ENOENT &&
                          fstatat(DIR, "moved/owned", &st, AT_SYMLINK_NOFOLLOW) == 0 && st.st_uid == 1000 && st.st_gid == 2000,
                      "symlink through renamed parent leaves reused name untouched");
        checks.expect(fstatat(parents[4], "absent", &st, AT_SYMLINK_NOFOLLOW) == -1 && errno == ENOENT,
                      "symlink missing ancestor remains absent");
    }
    for (size_t i = 0; i < parents.size(); ++i) {
        if (parents[i] < 0) continue;
        fchmod(parents[i], 0777);
        for (const char* name : {"exists", "new", "owned", "removable", "trailing", "original"}) unlinkat(parents[i], name, 0);
        close(parents[i]);
        unlinkat(DIR, NAMES[i], AT_REMOVEDIR);
    }
    if (DIR >= 0) {
        unlinkat(DIR, "moved", AT_REMOVEDIR);
        close(DIR);
    }
    checks.expect(rmdir(path) == 0, "remove symlink creation fixtures");
    return checks.finish();
}

auto directory_creation_policy(const char* base) -> int {
    Checks checks;
    char path[112];
    std::snprintf(path, sizeof(path), "%s/credentials-mkdir-%ld", base, static_cast<long>(getpid()));
    int const DIR = mkdir(path, 0755) == 0 ? open(path, O_RDONLY | O_DIRECTORY) : -1;
    constexpr std::array<const char*, 5> NAMES{"no-write", "no-search", "group", "inherit", "ordinary"};
    constexpr std::array<mode_t, 5> MODES{0555, 0666, 0770, 02777, 0777};
    constexpr std::array<gid_t, 5> GROUPS{0, 0, 3000, 4000, 0};
    std::array<int, 5> parents{-1, -1, -1, -1, -1};
    std::array<struct stat, 2> before{};
    bool ready = DIR >= 0;
    for (size_t i = 0; i < NAMES.size() && ready; ++i) {
        ready = mkdirat(DIR, NAMES[i], 0755) == 0;
        parents[i] = ready ? openat(DIR, NAMES[i], O_RDONLY | O_DIRECTORY) : -1;
        ready = parents[i] >= 0;
        if (i < 2) ready = ready && mkdirat(parents[i], "exists", 0755) == 0;
        ready = ready && fchown(parents[i], 0, GROUPS[i]) == 0 && fchmod(parents[i], MODES[i]) == 0;
        if (i < 2) ready = ready && fstat(parents[i], &before[i]) == 0;
    }
    ready = ready && renameat(DIR, "ordinary", DIR, "moved") == 0 && mkdirat(DIR, "ordinary", 0755) == 0;
    ready = ready && symlinkat("missing", parents[4], "dangling") == 0 && symlinkat("owned", parents[4], "ancestor") == 0;
    int const COLLISION = ready ? openat(parents[4], "file", O_CREAT | O_EXCL | O_RDWR, 0600) : -1;
    ready = ready && COLLISION >= 0;
    if (COLLISION >= 0) close(COLLISION);
    checks.expect(ready, "prepare protected, group, setgid and renamed mkdir parents");
    pid_t const CHILD = ready ? fork() : -1;
    if (CHILD == 0) {
        Checks child;
        std::array<gid_t, 1> const MEMBER{3000};
        if (setgroups(MEMBER.size(), MEMBER.data()) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        umask(0027);
        for (size_t i = 0; i < 2; ++i) {
            child.expect(mkdirat(parents[i], "new", 0777) == -1 && errno == EACCES,
                         "mkdir requires both parent write and search permissions");
            child.expect(mkdirat(parents[i], "exists", 0777) == -1 && errno == EACCES,
                         "existing child does not bypass parent mkdir authorization");
        }
        struct stat st{};
        child.expect(mkdirat(parents[2], "owned", 0777) == 0 && fstatat(parents[2], "owned", &st, 0) == 0 && st.st_uid == 1000 &&
                         st.st_gid == 2000 && (st.st_mode & 07777) == 0750,
                     "supplementary-group parent permits mkdir with effective IDs and umask");
        child.expect(mkdirat(parents[3], "owned", 0777) == 0 && fstatat(parents[3], "owned", &st, 0) == 0 && st.st_uid == 1000 &&
                         st.st_gid == 4000 && (st.st_mode & 07777) == 02750,
                     "setgid parent propagates its foreign group and setgid directory bit");
        int const INHERITED = openat(parents[3], "owned", O_RDONLY | O_DIRECTORY);
        child.expect(INHERITED >= 0 && mkdirat(INHERITED, "grandchild", 0777) == 0 && fstatat(INHERITED, "grandchild", &st, 0) == 0 &&
                         st.st_uid == 1000 && st.st_gid == 4000 && (st.st_mode & 07777) == 02750,
                     "setgid directory policy survives another generation");
        if (INHERITED >= 0) close(INHERITED);
        child.expect(mkdirat(parents[4], "owned", 0777) == 0 && fstatat(parents[4], "owned", &st, 0) == 0 && st.st_uid == 1000 &&
                         st.st_gid == 2000 && (st.st_mode & 07777) == 0750,
                     "mkdir through renamed parent uses retained permissions and creator identity");
        child.expect(mkdirat(parents[4], "zero", 0000) == 0 && fstatat(parents[4], "zero", &st, 0) == 0 && st.st_uid == 1000 &&
                         st.st_gid == 2000 && (st.st_mode & 07777) == 0,
                     "explicit zero directory permissions remain zero");
        child.expect(mkdirat(parents[4], "absent/deep", 0777) == -1 && errno == ENOENT && fstatat(parents[4], "absent", &st, 0) == -1 &&
                         errno == ENOENT,
                     "mkdir does not create missing intermediate directories");
        for (const char* name : {"dangling", "dangling/", "file", "file/"})
            child.expect(mkdirat(parents[4], name, 0777) == -1 && errno == EEXIST,
                         "mkdir final symlink and regular collisions return EEXIST with or without trailing slash");
        child.expect(fstatat(parents[4], "missing", &st, 0) == -1 && errno == ENOENT &&
                         fstatat(parents[4], "dangling", &st, AT_SYMLINK_NOFOLLOW) == 0 && S_ISLNK(st.st_mode),
                     "mkdir leaves dangling final link and its absent target unchanged");
        child.expect(mkdirat(parents[4], "ancestor/through", 0777) == 0 && fstatat(parents[4], "owned/through", &st, 0) == 0 &&
                         st.st_uid == 1000 && st.st_gid == 2000 && (st.st_mode & 07777) == 0750,
                     "mkdir still follows intermediate directory symlinks with prepared creator identity");
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "directory creation credential subprocess");
    if (ready) {
        struct stat st{};
        for (size_t i = 0; i < 2; ++i) {
            checks.expect(fstat(parents[i], &st) == 0 && st.st_ino == before[i].st_ino && st.st_nlink == before[i].st_nlink &&
                              st.st_mode == before[i].st_mode && same_times(st, before[i]) && fstatat(parents[i], "new", &st, 0) == -1 &&
                              errno == ENOENT,
                          "denied mkdir leaves parent metadata and namespace unchanged");
        }
        checks.expect(fstatat(DIR, "ordinary/owned", &st, 0) == -1 && errno == ENOENT && fstatat(DIR, "moved/owned", &st, 0) == 0 &&
                          st.st_uid == 1000 && st.st_gid == 2000,
                      "reused parent name remains untouched while original directory gains the child");
    }
    for (size_t i = 0; i < parents.size(); ++i) {
        if (parents[i] < 0) continue;
        fchmod(parents[i], 0777);
        if (i == 3) {
            int const OWNED = openat(parents[i], "owned", O_RDONLY | O_DIRECTORY);
            if (OWNED >= 0) {
                unlinkat(OWNED, "grandchild", AT_REMOVEDIR);
                close(OWNED);
            }
        }
        if (i == 4) {
            int const OWNED = openat(parents[i], "owned", O_RDONLY | O_DIRECTORY);
            if (OWNED >= 0) {
                unlinkat(OWNED, "through", AT_REMOVEDIR);
                close(OWNED);
            }
            unlinkat(parents[i], "dangling", 0);
            unlinkat(parents[i], "ancestor", 0);
            unlinkat(parents[i], "file", 0);
            unlinkat(parents[i], "missing", AT_REMOVEDIR);
        }
        unlinkat(parents[i], "exists", AT_REMOVEDIR);
        unlinkat(parents[i], "new", AT_REMOVEDIR);
        unlinkat(parents[i], "owned", AT_REMOVEDIR);
        unlinkat(parents[i], "zero", AT_REMOVEDIR);
        close(parents[i]);
        unlinkat(DIR, NAMES[i], AT_REMOVEDIR);
    }
    if (DIR >= 0) {
        unlinkat(DIR, "moved", AT_REMOVEDIR);
        close(DIR);
    }
    checks.expect(rmdir(path) == 0, "remove directory creation fixtures");
    return checks.finish();
}

auto mount_administration_policy() -> int {
    Checks checks;
    char base[112], active[144], target[144], missing[144], marker[176];
    std::snprintf(base, sizeof(base), "/tmp/credential-mount-%ld", getpid());
    std::snprintf(active, sizeof(active), "%s/active", base);
    std::snprintf(target, sizeof(target), "%s/target", base);
    std::snprintf(missing, sizeof(missing), "%s/missing", base);
    std::snprintf(marker, sizeof(marker), "%s/marker", active);
    bool ready = mkdir(base, 0777) == 0 && chmod(base, 0777) == 0 && mkdir(active, 0755) == 0 && mkdir(target, 0755) == 0;
    struct stat before_target{}, before_root{}, before_mount{};
    ready = ready && stat(target, &before_target) == 0 && stat("/", &before_root) == 0;
    checks.expect(ready, "prepare mount-administration sentinels");
    if (!ready) return checks.finish();
    bool const MOUNTED = mount("none", active, "tmpfs", 0, nullptr) == 0;
    checks.expect(MOUNTED, "root with MOUNT authority can mount");
    if (!MOUNTED) return checks.finish();
    int fd = open(marker, O_CREAT | O_EXCL | O_RDWR, 0644);
    ready = fd >= 0 && write(fd, "retained-mount", 14) == 14 && stat(active, &before_mount) == 0;
    if (fd >= 0) close(fd);
    checks.expect(ready, "prepare live mount identity and data");
    pid_t const CHILD = ready ? fork() : -1;
    if (CHILD == 0) {
        Checks child;
        if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        child.expect(mount("none", target, "tmpfs", 0, nullptr) == -1 && errno == EPERM, "unprivileged mount cannot cover existing target");
        child.expect(mount("none", missing, "tmpfs", 0, nullptr) == -1 && errno == EPERM, "mount denial precedes target creation");
        child.expect(mount("wki://goal19-missing-peer/export", missing, "remote", 0, nullptr) == -1 && errno == EPERM,
                     "mount denial precedes remote peer discovery");
        child.expect(mount("/dev/goal19-missing-device", missing, "fat32", 0, nullptr) == -1 && errno == EPERM,
                     "mount denial precedes block device resolution");
        child.expect(umount(active) == -1 && errno == EPERM, "unprivileged unmount cannot retire live mount");
        uint64_t pivot = static_cast<uint64_t>(ker::abi::callnums::VFS);
        asm volatile("syscall"
                     : "+a"(pivot)
                     : "D"(static_cast<uint64_t>(ker::abi::vfs::ops::PIVOT_ROOT)), "S"(missing), "d"("/goal19-old-root")
                     : "rcx", "r11", "memory");
        child.expect(static_cast<int64_t>(pivot) == -EPERM, "pivot denial precedes path validation and global root work");
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "mount administration denial subprocess");
    struct stat after{};
    checks.expect(stat(target, &after) == 0 && after.st_ino == before_target.st_ino && after.st_dev == before_target.st_dev &&
                      after.st_mode == before_target.st_mode && same_times(after, before_target),
                  "denied mount leaves existing target metadata unchanged");
    checks.expect(stat(missing, &after) == -1 && errno == ENOENT, "denied mount leaves absent target absent");
    checks.expect(stat(active, &after) == 0 && after.st_dev == before_mount.st_dev && after.st_ino == before_mount.st_ino,
                  "denied unmount preserves mounted identity");
    checks.expect(stat("/", &after) == 0 && after.st_dev == before_root.st_dev && after.st_ino == before_root.st_ino,
                  "denied pivot leaves process root unchanged");
    char contents[16]{};
    fd = open(marker, O_RDONLY);
    checks.expect(fd >= 0 && read(fd, contents, sizeof(contents)) == 14 && std::memcmp(contents, "retained-mount", 14) == 0,
                  "denied administration preserves mounted file contents");
    if (fd >= 0) close(fd);
    checks.expect(umount(active) == 0, "root with MOUNT authority can unmount");
    checks.expect(rmdir(active) == 0 && rmdir(target) == 0 && rmdir(base) == 0, "remove mount-administration fixtures");
    return checks.finish();
}

auto mount_target_policy(const char* base) -> int {
    Checks checks;
    char path[112], target[144], missing[144], file[144], link[144];
    std::snprintf(path, sizeof(path), "%s/credential-mount-target-%ld", base, getpid());
    std::snprintf(target, sizeof(target), "%s/target", path);
    std::snprintf(missing, sizeof(missing), "%s/missing", path);
    std::snprintf(file, sizeof(file), "%s/file", path);
    std::snprintf(link, sizeof(link), "%s/link", path);
    bool ready = mkdir(path, 0755) == 0 && mkdir(target, 0755) == 0;
    int fd = ready ? open(file, O_CREAT | O_EXCL | O_WRONLY, 0644) : -1;
    ready = fd >= 0 && write(fd, "target", 6) == 6;
    if (fd >= 0) close(fd);
    ready = ready && symlink("file", link) == 0;
    struct stat parent{}, directory{}, regular{}, after{};
    ready = ready && stat(path, &parent) == 0 && stat(target, &directory) == 0 && stat(file, &regular) == 0;
    checks.expect(ready, "prepare mount-target failure sentinels");
    if (!ready) return checks.finish();
    checks.expect(mount("none", missing, "tmpfs", 0, nullptr) == -1 && errno == ENOENT, "root mount requires an explicitly created target");
    checks.expect(stat(missing, &after) == -1 && errno == ENOENT, "failed root mount leaves missing target absent");
    checks.expect(mount("none", file, "tmpfs", 0, nullptr) == -1 && errno == ENOTDIR, "mount cannot cover a regular file");
    checks.expect(mount("none", link, "tmpfs", 0, nullptr) == -1 && errno == ENOTDIR,
                  "mount follows target symlink and rejects its regular file");
    checks.expect(mount("none", target, "unknown-credential-fs", 0, nullptr) == -1 && errno == ENODEV,
                  "unknown filesystem fails without changing existing target");
    checks.expect(mount("/dev/goal19-missing-device", target, "fat32", 0, nullptr) == -1 && errno == ENOENT,
                  "missing device fails with existing target intact");
    checks.expect(mount("wki://goal19-missing-peer/export", target, "remote", 0, nullptr) == -1 && errno == ENODEV,
                  "missing peer fails with existing target intact");
    checks.expect(
        stat(path, &after) == 0 && after.st_ino == parent.st_ino && after.st_nlink == parent.st_nlink && same_times(after, parent),
        "failed mount leaves parent identity links and timestamps unchanged");
    checks.expect(stat(target, &after) == 0 && after.st_ino == directory.st_ino && after.st_dev == directory.st_dev &&
                      after.st_mode == directory.st_mode && same_times(after, directory),
                  "failed preparation leaves target metadata unchanged");
    checks.expect(
        stat(file, &after) == 0 && after.st_ino == regular.st_ino && after.st_size == regular.st_size && same_times(after, regular),
        "rejected file target retains identity size and timestamps");
    char contents[8]{};
    fd = open(file, O_RDONLY);
    checks.expect(fd >= 0 && read(fd, contents, sizeof(contents)) == 6 && std::memcmp(contents, "target", 6) == 0,
                  "rejected file target retains data");
    if (fd >= 0) close(fd);
    checks.expect(unlink(link) == 0 && symlink("target", link) == 0, "prepare directory target symlink");
    bool const MOUNTED = mount("none", link, "tmpfs", 0, nullptr) == 0;
    checks.expect(MOUNTED, "root mount follows an existing directory symlink");
    if (MOUNTED) checks.expect(umount(target) == 0, "unmount resolved directory target");
    checks.expect(unlink(link) == 0 && unlink(file) == 0 && rmdir(target) == 0 && rmdir(path) == 0, "remove mount-target fixtures");
    return checks.finish();
}

auto mount_credential_race(const char* base) -> int {
    Checks checks;
    char path[112];
    std::snprintf(path, sizeof(path), "%s/credential-mount-race-%ld", base, getpid());
    struct stat original{}, after{};
    checks.expect(mkdir(path, 0755) == 0 && stat(path, &original) == 0, "prepare mount credential-race target");
    if (original.st_ino == 0) return checks.finish();
    unsigned mounted = 0, denied = 0, retried = 0, changes = 0;
    for (unsigned round = 0; round < 24; ++round) {
        struct Race {
            std::atomic<bool> ready{false}, start{false}, stop{false};
            unsigned changes{};
            int error{};
        } race;
        auto flip = [](void* opaque) -> void* {
            auto& state = *static_cast<Race*>(opaque);
            state.ready.store(true, std::memory_order_release);
            while (!state.start.load(std::memory_order_acquire)) sched_yield();
            while (!state.stop.load(std::memory_order_acquire)) {
                if (seteuid(1000) != 0 || seteuid(0) != 0) {
                    state.error = errno;
                    break;
                }
                ++state.changes;
            }
            if (seteuid(0) != 0) state.error = errno;
            return nullptr;
        };
        pthread_t thread{};
        int const CREATED = pthread_create(&thread, nullptr, flip, &race);
        checks.expect(CREATED == 0, "start mount credential-race sibling");
        if (CREATED != 0) break;
        while (!race.ready.load(std::memory_order_acquire)) sched_yield();
        race.start.store(true, std::memory_order_release);
        int const RESULT = mount("none", path, "tmpfs", 0, nullptr);
        int const ERROR = errno;
        race.stop.store(true, std::memory_order_release);
        int const JOINED = pthread_join(thread, nullptr);
        checks.expect(JOINED == 0 && race.error == 0 && geteuid() == 0, "join sibling with effective root restored");
        if (JOINED != 0 || seteuid(0) != 0) return checks.finish();
        changes += race.changes;
        if (RESULT == 0) {
            ++mounted;
            bool const UNMOUNTED = umount(path) == 0;
            checks.expect(UNMOUNTED, "successful raced mount can be unmounted");
            if (!UNMOUNTED) return checks.finish();
        } else {
            denied += ERROR == EPERM;
            retried += ERROR == EAGAIN;
            checks.expect(RESULT == -1 && (ERROR == EPERM || ERROR == EAGAIN), "raced mount fails only admission or stale snapshot");
            if (ERROR != EPERM && ERROR != EAGAIN) std::printf("credentials: mount race unexpected errno=%d round=%u\n", ERROR, round);
        }
        checks.expect(stat(path, &after) == 0 && after.st_dev == original.st_dev && after.st_ino == original.st_ino &&
                          after.st_mode == original.st_mode && same_times(after, original),
                      "raced mount leaves no failed publication or target metadata change");
    }
    std::printf("credentials: mount race base=%s mounted=%u denied=%u stale=%u credential_changes=%u\n", base, mounted, denied, retried,
                changes);
    checks.expect(mounted + denied + retried == 24 && changes != 0, "complete mount race with concurrent credential changes");
    checks.expect(rmdir(path) == 0, "remove mount credential-race target");
    return checks.finish();
}

auto retained_cwd_policy(const char* base) -> int {
    Checks checks;
    char path[112], old[144], moved[144], file_path[176];
    std::snprintf(path, sizeof(path), "%s/credential-cwd-%ld", base, getpid());
    std::snprintf(old, sizeof(old), "%s/old", path);
    std::snprintf(moved, sizeof(moved), "%s/moved", path);
    std::snprintf(file_path, sizeof(file_path), "%s/file", old);
    bool ready = mkdir(path, 0755) == 0 && mkdir(old, 0755) == 0;
    int fd = ready ? open(file_path, O_CREAT | O_EXCL | O_RDWR, 0644) : -1;
    struct stat original{};
    ready = fd >= 0 && write(fd, "original", 8) == 8 && fstat(fd, &original) == 0;
    if (fd >= 0) close(fd);
    checks.expect(ready, "prepare retained cwd fixture");
    if (!ready) return checks.finish();
    checks.expect(chdir(old) == 0 && rename(old, moved) == 0 && mkdir(old, 0755) == 0, "rename and reuse cwd pathname");
    fd = open(file_path, O_CREAT | O_EXCL | O_RDWR, 0644);
    checks.expect(fd >= 0 && write(fd, "replacement", 11) == 11, "create replacement directory content");
    if (fd >= 0) close(fd);
    auto original_visible = [&]() {
        int const INPUT = open("file", O_RDONLY);
        struct stat current{};
        char data[12]{};
        bool const OK = INPUT >= 0 && fstat(INPUT, &current) == 0 && current.st_ino == original.st_ino &&
                        read(INPUT, data, sizeof(data)) == 8 && std::memcmp(data, "original", 8) == 0;
        if (INPUT >= 0) close(INPUT);
        return OK;
    };
    checks.expect(original_visible(), "cwd retains inode after pathname reuse");
    char cwd[256]{};
    checks.expect(getcwd(cwd, sizeof(cwd)) != nullptr && std::strcmp(cwd, moved) == 0, "getcwd recovers renamed identity");
    pid_t const CHILD = fork();
    if (CHILD == 0) _exit(original_visible() ? 0 : 1);
    int status = 0;
    checks.expect(CHILD > 0 && waitpid(CHILD, &status, 0) == CHILD && WIFEXITED(status) && WEXITSTATUS(status) == 0,
                  "fork inherits retained cwd rather than stale pathname");
    checks.expect(chdir(path) == 0, "prepare shared process cwd");
    pthread_t thread{};
    auto change = [](void* target) -> void* { return reinterpret_cast<void*>(static_cast<intptr_t>(chdir(static_cast<char*>(target)))); };
    int const CREATED = pthread_create(&thread, nullptr, change, moved);
    void* result = reinterpret_cast<void*>(1);
    checks.expect(CREATED == 0, "create cwd-changing sibling");
    if (CREATED == 0) checks.expect(pthread_join(thread, &result) == 0 && result == nullptr, "sibling changes cwd");
    checks.expect(getcwd(cwd, sizeof(cwd)) != nullptr && std::strcmp(cwd, moved) == 0 && original_visible(),
                  "sibling cwd is shared by process");
    checks.expect(chmod(moved, 0111) == 0, "prepare execute-only cwd");
    pid_t const USER = fork();
    if (USER == 0) {
        if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        _exit(chdir(".") == 0 && original_visible() ? 0 : 1);
    }
    status = 0;
    checks.expect(USER > 0 && waitpid(USER, &status, 0) == USER && WIFEXITED(status) && WEXITSTATUS(status) == 0,
                  "cwd pin requires search without granting or requiring directory read");
    checks.expect(chmod(moved, 0755) == 0 && chdir("/") == 0, "leave retained cwd before cleanup");
    unlink(file_path);
    std::snprintf(file_path, sizeof(file_path), "%s/file", moved);
    unlink(file_path);
    rmdir(old);
    rmdir(moved);
    rmdir(path);
    return checks.finish();
}

auto original_component_policy(const char* base) -> int {
    Checks checks;
    char path[112], locked[144], child_path[160], public_path[144], file_path[176];
    std::snprintf(path, sizeof(path), "%s/credentials-components-%ld", base, static_cast<long>(getpid()));
    std::snprintf(locked, sizeof(locked), "%s/locked", path);
    std::snprintf(child_path, sizeof(child_path), "%s/child", locked);
    std::snprintf(public_path, sizeof(public_path), "%s/public", path);
    std::snprintf(file_path, sizeof(file_path), "%s/file", public_path);
    bool ready = mkdir(path, 0755) == 0 && mkdir(locked, 0700) == 0 && mkdir(child_path, 0755) == 0 && mkdir(public_path, 0777) == 0 &&
                 chmod(public_path, 0777) == 0;
    int const ANCHOR = ready ? open(child_path, O_RDONLY | O_DIRECTORY) : -1;
    int const PUBLIC = ready ? open(public_path, O_RDONLY | O_DIRECTORY) : -1;
    int const PRIVATE_FILE = ANCHOR >= 0 ? openat(ANCHOR, "file", O_CREAT | O_EXCL | O_RDWR, 0644) : -1;
    int const FILE = PUBLIC >= 0 ? openat(PUBLIC, "file", O_CREAT | O_EXCL | O_RDWR, 0644) : -1;
    struct stat before{};
    ready = PRIVATE_FILE >= 0 && FILE >= 0 && write(PRIVATE_FILE, "search", 6) == 6 && write(FILE, "search", 6) == 6 &&
            fstat(FILE, &before) == 0 && symlinkat("./file", ANCHOR, "relative") == 0 && symlinkat(file_path, ANCHOR, "absolute") == 0 &&
            symlinkat("../locked/child/file", PUBLIC, "hidden") == 0 && symlinkat(".", PUBLIC, "cwd-link") == 0 &&
            mkdirat(PUBLIC, "empty", 0755) == 0 && symlinkat("empty", PUBLIC, "empty-link") == 0;
    char escape[176];
    std::snprintf(escape, sizeof(escape), "%s/escape", locked);
    ready = ready && symlink(file_path, escape) == 0;
    checks.expect(ready, "prepare component-walk authority fixtures");
    pid_t const CHILD = ready ? fork() : -1;
    if (CHILD == 0) {
        Checks child;
        if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
        for (const char* suffix : {"/locked/child/file", "/locked/../public/file", "/locked/escape", "/public/hidden",
                                   "/locked/./child/file", "/public/../locked/child/file"}) {
            char denied[208];
            std::snprintf(denied, sizeof(denied), "%s%s", path, suffix);
            struct stat state{};
            for (unsigned repeat = 0; repeat < 2; ++repeat) {
                child.expect(stat(denied, &state) == -1 && errno == EACCES, "original path components precede cached stat authority");
                int const FD = open(denied, O_RDONLY);
                child.expect(FD == -1 && errno == EACCES, "symlink and dotdot cannot skip an inaccessible original ancestor");
                if (FD >= 0) close(FD);
            }
        }
        for (const char* name : {"file", "./file", ".//./file", "relative", "absolute"}) {
            int const FD = openat(ANCHOR, name, O_RDONLY);
            char data[6]{};
            child.expect(FD >= 0 && read(FD, data, sizeof(data)) == sizeof(data) && std::memcmp(data, "search", 6) == 0,
                         "retained directory and symlink restarts preserve the selected starting point");
            if (FD >= 0) close(FD);
        }
        char denied[208], created[208];
        std::snprintf(denied, sizeof(denied), "%s/locked/../public/file", path);
        std::snprintf(created, sizeof(created), "%s/locked/../public/new", path);
        child.expect(unlink(denied) == -1 && errno == EACCES, "unlink cannot normalize away a denied ancestor");
        child.expect(mkdir(created, 0700) == -1 && errno == EACCES, "mkdir denial precedes target creation");
        child.expect(symlink("file", created) == -1 && errno == EACCES, "symlink denial precedes target creation");
        child.expect(rename(denied, created) == -1 && errno == EACCES, "rename denial leaves both bindings unchanged");
        std::snprintf(denied, sizeof(denied), "%s/public/file/../file", path);
        struct stat state{};
        child.expect(stat(denied, &state) == -1 && errno == ENOTDIR, "dotdot cannot turn a regular file into a directory");
        child.expect(fchdir(ANCHOR) == 0, "fchdir uses retained identity below hidden ancestors");
        char cwd[208]{};
        child.expect(getcwd(cwd, sizeof(cwd)) != nullptr && std::strcmp(cwd, child_path) == 0, "cwd records the resolved directory");
        child.expect(chdir("../child") == -1 && errno == EACCES && getcwd(cwd, sizeof(cwd)) != nullptr && std::strcmp(cwd, child_path) == 0,
                     "failed chdir leaves cwd unchanged");
        std::snprintf(denied, sizeof(denied), "%s/public/cwd-link", path);
        child.expect(chdir(denied) == 0 && getcwd(cwd, sizeof(cwd)) != nullptr && std::strcmp(cwd, public_path) == 0,
                     "chdir through symlink stores physical cwd");
        int const RESULT = child.finish();
        std::fflush(nullptr);
        _exit(RESULT);
    }
    checks.expect(wait_ok(CHILD), "original-component credential subprocess");
    if (ready) {
        struct stat after{};
        char data[6]{};
        checks.expect(fstat(FILE, &after) == 0 && after.st_ino == before.st_ino && after.st_size == before.st_size &&
                          same_times(after, before) && pread(FILE, data, sizeof(data), 0) == sizeof(data) &&
                          std::memcmp(data, "search", 6) == 0 && fstatat(PUBLIC, "new", &after, 0) == -1 && errno == ENOENT,
                      "denied component mutations preserve names metadata and bytes");
        checks.expect(unlinkat(PUBLIC, "file/", 0) == -1 && errno == ENOTDIR, "unlink keeps a directory requirement");
        checks.expect(unlinkat(PUBLIC, "empty-link", AT_REMOVEDIR) == -1 && errno == ENOTDIR &&
                          unlinkat(PUBLIC, "empty-link/", AT_REMOVEDIR) == -1 && errno == ENOTDIR &&
                          fstatat(PUBLIC, "empty", &after, 0) == 0 && S_ISDIR(after.st_mode),
                      "rmdir preserves final symlink and its directory target");
        checks.expect(unlinkat(PUBLIC, ".", AT_REMOVEDIR) == -1 && errno == EINVAL, "rmdir rejects dot without detaching the directory");
        checks.expect(fchmod(ANCHOR, 0700) == 0, "revoke retained cwd anchor search");
        pid_t const REVOKED = fork();
        if (REVOKED == 0) {
            Checks child;
            if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
            child.expect(fchdir(ANCHOR) == -1 && errno == EACCES, "retained fchdir rechecks search permission");
            child.expect(fchdir(AT_FDCWD) == -1 && errno == EBADF, "fchdir rejects the pathname-only AT_FDCWD sentinel");
            int const RESULT = child.finish();
            std::fflush(nullptr);
            _exit(RESULT);
        }
        checks.expect(wait_ok(REVOKED), "revoked fchdir credential subprocess");
        fchmod(ANCHOR, 0755);
    }
    if (PRIVATE_FILE >= 0) close(PRIVATE_FILE);
    if (FILE >= 0) close(FILE);
    if (ANCHOR >= 0) {
        for (const char* name : {"file", "relative", "absolute"}) unlinkat(ANCHOR, name, 0);
        close(ANCHOR);
    }
    if (PUBLIC >= 0) {
        for (const char* name : {"file", "hidden", "cwd-link", "empty-link", "new"}) unlinkat(PUBLIC, name, 0);
        unlinkat(PUBLIC, "empty", AT_REMOVEDIR);
        close(PUBLIC);
    }
    unlink(escape);
    rmdir(child_path);
    rmdir(locked);
    rmdir(public_path);
    checks.expect(rmdir(path) == 0, "remove original-component fixtures");
    return checks.finish();
}

auto directory_search_policy(const char* base) -> int {
    Checks checks;
    char path[112], leaf[144];
    std::snprintf(path, sizeof(path), "%s/credentials-search-%ld", base, static_cast<long>(getpid()));
    std::snprintf(leaf, sizeof(leaf), "%s/child/file", path);
    int const BASE = mkdir(path, 0700) == 0 ? open(path, O_RDONLY | O_DIRECTORY) : -1;
    bool ready = BASE >= 0 && mkdirat(BASE, "child", 0755) == 0;
    int const DIR = ready ? openat(BASE, "child", O_RDONLY | O_DIRECTORY) : -1;
    int const FILE = DIR >= 0 ? openat(DIR, "file", O_CREAT | O_EXCL | O_RDWR, 0644) : -1;
    struct stat original{};
    ready = FILE >= 0 && write(FILE, "search", 6) == 6 && fstat(FILE, &original) == 0;
    checks.expect(ready, "prepare hidden ancestor and retained searchable directory");
    for (unsigned round = 0; ready && round < 2; ++round) {
        // The same retained FD must obey changes to its own directory mode.
        ready = fchmod(DIR, round == 0 ? 0755 : 0700) == 0;
        checks.expect(ready, "set retained directory search mode");
        pid_t const CHILD = ready ? fork() : -1;
        if (CHILD == 0) {
            Checks child;
            if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
            for (unsigned repeat = 0; repeat < 3; ++repeat) {
                struct stat state{};
                child.expect(stat(leaf, &state) == -1 && errno == EACCES, "stat cannot bypass hidden ancestor through warm cache");
                int fd = open(leaf, O_RDONLY);
                child.expect(fd == -1 && errno == EACCES, "open checks hidden ancestor before public target");
                if (fd >= 0) close(fd);
                fd = openat(DIR, "file", O_RDONLY);
                char data[6]{};
                if (round == 0) {
                    child.expect(fd >= 0 && fstat(fd, &state) == 0 && state.st_ino == original.st_ino &&
                                     read(fd, data, sizeof(data)) == sizeof(data) && std::memcmp(data, "search", 6) == 0,
                                 "retained directory starts lookup below inaccessible ancestors");
                    child.expect(fstatat(DIR, "file", &state, 0) == 0 && state.st_ino == original.st_ino,
                                 "retained stat uses the same authorized directory anchor");
                } else {
                    child.expect(fd == -1 && errno == EACCES, "retained directory does not retain discarded search permission");
                    child.expect(fstatat(DIR, "file", &state, 0) == -1 && errno == EACCES, "warm retained stat rechecks directory search");
                }
                if (fd >= 0) close(fd);
            }
            int const RESULT = child.finish();
            std::fflush(nullptr);
            _exit(RESULT);
        }
        checks.expect(wait_ok(CHILD), "directory search credential subprocess");
    }
    if (FILE >= 0) close(FILE);
    if (DIR >= 0) {
        fchmod(DIR, 0755);
        unlinkat(DIR, "file", 0);
        close(DIR);
    }
    if (BASE >= 0) {
        unlinkat(BASE, "child", AT_REMOVEDIR);
        close(BASE);
    }
    checks.expect(rmdir(path) == 0, "remove directory search fixtures");
    return checks.finish();
}

auto directory_identity_policy(const char* base) -> int {
    Checks checks;
    char original[96], moved[96], alternate[96], replacement_branch[128], replacement_file[160];
    std::snprintf(original, sizeof(original), "%s/credentials-dirfd-%ld", base, static_cast<long>(getpid()));
    std::snprintf(moved, sizeof(moved), "%s/credentials-dirfd-moved-%ld", base, static_cast<long>(getpid()));
    std::snprintf(alternate, sizeof(alternate), "%s/credentials-dirfd-alternate-%ld", base, static_cast<long>(getpid()));
    std::snprintf(replacement_branch, sizeof(replacement_branch), "%s/branch", original);
    std::snprintf(replacement_file, sizeof(replacement_file), "%s/original", replacement_branch);
    bool ready = mkdir(original, 0755) == 0;
    int const ROOT = ready ? open(original, O_RDONLY | O_DIRECTORY) : -1;
    ready = ROOT >= 0 && mkdirat(ROOT, "branch", 0777) == 0;
    int const BRANCH = ready ? openat(ROOT, "branch", O_RDONLY | O_DIRECTORY) : -1;
    int const FILE = BRANCH >= 0 ? openat(BRANCH, "original", O_CREAT | O_EXCL | O_RDWR, 0600) : -1;
    constexpr char DATA[] = "retained directory inode";
    ready =
        FILE >= 0 && write(FILE, DATA, sizeof(DATA)) == sizeof(DATA) && fchmod(BRANCH, 01777) == 0 && mkdirat(BRANCH, "nested", 0777) == 0;
    int const NESTED = ready ? openat(BRANCH, "nested", O_RDONLY | O_DIRECTORY) : -1;
    ready = NESTED >= 0 && symlinkat("../original", NESTED, "up") == 0;
    checks.expect(ready, "nested directory opens remain usable after mkdir changes namespace generation");
    ready = ready && rename(original, moved) == 0 && mkdir(original, 0755) == 0 && mkdir(replacement_branch, 0755) == 0;
    int const REPLACEMENT = ready ? open(replacement_file, O_CREAT | O_EXCL | O_RDWR, 0600) : -1;
    constexpr char OTHER[] = "replacement must survive";
    struct stat replacement_before{}, original_stat{}, branch_stat{};
    ready = REPLACEMENT >= 0 && write(REPLACEMENT, OTHER, sizeof(OTHER)) == sizeof(OTHER) && fstat(REPLACEMENT, &replacement_before) == 0 &&
            fstat(FILE, &original_stat) == 0 && fstat(BRANCH, &branch_stat) == 0;
    checks.expect(ready, "rename retained tree and reuse its historical pathname with a different inode");
    if (ready) {
        struct stat changed{};
        checks.expect(fchmodat(BRANCH, "original", 0640, 0) == 0 && fstat(FILE, &changed) == 0 && changed.st_ino == original_stat.st_ino &&
                          (changed.st_mode & 07777) == 0640 && fchmodat(BRANCH, "original", 0600, 0) == 0 && fstat(FILE, &changed) == 0 &&
                          (changed.st_mode & 07777) == 0600,
                      "retained fstat sees metadata changes through the renamed tree's current name");
        int const REOPENED = openat(ROOT, "branch", O_RDONLY | O_DIRECTORY);
        int const LEAF = REOPENED >= 0 ? openat(REOPENED, "original", O_RDONLY) : -1;
        struct stat observed{};
        std::array<char, sizeof(DATA)> output{};
        checks.expect(LEAF >= 0 && fstat(LEAF, &observed) == 0 && observed.st_ino == original_stat.st_ino &&
                          pread(LEAF, output.data(), output.size(), 0) == output.size() &&
                          std::memcmp(output.data(), DATA, sizeof(DATA)) == 0,
                      "descendant directory opened after rename stays with retained tree rather than replacement");
        if (LEAF >= 0) close(LEAF);
        if (REOPENED >= 0) close(REOPENED);
        int const DOT = openat(NESTED, ".", O_RDONLY | O_DIRECTORY);
        int const PARENT = openat(NESTED, "..", O_RDONLY | O_DIRECTORY);
        checks.expect(DOT >= 0 && PARENT >= 0 && fstat(PARENT, &observed) == 0 && observed.st_ino == branch_stat.st_ino,
                      "dot and parent traversal use the current retained directory path");
        if (DOT >= 0) close(DOT);
        if (PARENT >= 0) close(PARENT);
        int const LINK = openat(NESTED, "up", O_RDONLY);
        checks.expect(LINK >= 0 && fstat(LINK, &observed) == 0 && observed.st_ino == original_stat.st_ino,
                      "relative parent symlink follows the retained tree after rename");
        if (LINK >= 0) close(LINK);
        pid_t const CHILD = fork();
        if (CHILD == 0) {
            Checks child;
            if (setgroups(0, nullptr) != 0 || setgid(2000) != 0 || setuid(1000) != 0) _exit(1);
            int const DENIED = openat(BRANCH, "original", O_RDONLY);
            child.expect(DENIED == -1 && errno == EACCES, "recovered dirfd path keeps private target read authorization");
            if (DENIED >= 0) close(DENIED);
            child.expect(unlinkat(BRANCH, "original", 0) == -1 && errno == EPERM,
                         "recovered dirfd path keeps sticky removal authorization");
            int const RESULT = child.finish();
            std::fflush(nullptr);
            _exit(RESULT);
        }
        checks.expect(wait_ok(CHILD), "retained-directory credential denial subprocess");
        pid_t const RENAMER = fork();
        if (RENAMER == 0) {
            unsigned retries = 0;
            auto move = [&](const char* source, const char* target) {
                for (unsigned attempt = 0; attempt < 128; ++attempt) {
                    if (rename(source, target) == 0) return true;
                    int const ERROR = errno;
                    if (ERROR != EAGAIN) {
                        std::fprintf(stderr, "credentials: dirfd renamer failure base=%s errno=%d source=%s target=%s\n", base, ERROR,
                                     source, target);
                        return false;
                    }
                    ++retries;
                }
                std::fprintf(stderr, "credentials: dirfd renamer retry exhaustion base=%s\n", base);
                return false;
            };
            for (unsigned i = 0; i < 100; ++i)
                if (!move(moved, alternate) || !move(alternate, moved)) _exit(1);
            std::printf("credentials: dirfd renamer base=%s moves=200 retries=%u\n", base, retries);
            std::fflush(nullptr);
            _exit(0);
        }
        unsigned samples = 0, retries = 0;
        bool correct = RENAMER >= 0;
        for (unsigned i = 0; i < 200 && correct; ++i) {
            int const FD = openat(BRANCH, "original", O_RDONLY);
            if (FD == -1 && errno == EAGAIN) {
                ++retries;
                continue;
            }
            int const OPEN_ERROR = FD < 0 ? errno : 0;
            int const STAT_RESULT = FD >= 0 ? fstat(FD, &observed) : -1;
            correct = FD >= 0 && STAT_RESULT == 0 && observed.st_ino == original_stat.st_ino;
            if (!correct)
                std::printf("credentials: dirfd race failure base=%s fd=%d open_errno=%d stat=%d errno=%d inode=%lu expected=%lu\n", base,
                            FD, OPEN_ERROR, STAT_RESULT, errno, static_cast<unsigned long>(observed.st_ino),
                            static_cast<unsigned long>(original_stat.st_ino));
            if (FD >= 0) {
                ++samples;
                close(FD);
            }
        }
        checks.expect(wait_ok(RENAMER) && correct && samples != 0, "rename race preserves exact directory target or reports retry");
        std::printf("credentials: dirfd rename race base=%s samples=%u retries=%u\n", base, samples, retries);
        std::array<char, sizeof(OTHER)> replacement_output{};
        checks.expect(stat(replacement_file, &observed) == 0 && observed.st_ino == replacement_before.st_ino &&
                          observed.st_size == replacement_before.st_size && same_times(observed, replacement_before) &&
                          pread(REPLACEMENT, replacement_output.data(), replacement_output.size(), 0) == replacement_output.size() &&
                          std::memcmp(replacement_output.data(), OTHER, sizeof(OTHER)) == 0,
                      "reused pathname remains untouched by retained-directory operations and denials");
        checks.expect(unlinkat(NESTED, "up", 0) == 0 && unlinkat(BRANCH, "nested", AT_REMOVEDIR) == 0,
                      "remove retained empty nested directory");
        int const REMOVED = openat(NESTED, "new", O_CREAT | O_EXCL | O_RDWR, 0600);
        checks.expect(REMOVED == -1 && errno == ENOENT && fstat(NESTED, &observed) == 0,
                      "removed directory cannot create names but keeps its descriptor identity");
        if (REMOVED >= 0) close(REMOVED);
    }
    if (FILE >= 0) close(FILE);
    if (NESTED >= 0) {
        unlinkat(NESTED, "up", 0);
        close(NESTED);
    }
    if (BRANCH >= 0) {
        unlinkat(BRANCH, "original", 0);
        unlinkat(BRANCH, "nested", AT_REMOVEDIR);
        close(BRANCH);
    }
    if (ROOT >= 0) {
        unlinkat(ROOT, "branch", AT_REMOVEDIR);
        close(ROOT);
    }
    if (REPLACEMENT >= 0) close(REPLACEMENT);
    unlink(replacement_file);
    rmdir(replacement_branch);
    rmdir(original);
    rmdir(alternate);
    checks.expect(rmdir(moved) == 0, "remove exact retained-directory fixture");
    return checks.finish();
}

auto ordinary_exec() -> int {
    if (setgid(2000) != 0 || setegid(2001) != 0 || seteuid(1000) != 0) return 1;
    execl("/usr/bin/testprog", "testprog", "credentials", "exec-check", static_cast<char*>(nullptr));
    return 1;
}
}  // namespace

auto run_credentials(int argc, char** argv) -> int {
    if (argc == 2 && std::strcmp(argv[0], "delegation-service-check") == 0 &&
        (std::strcmp(argv[1], "1") == 0 || std::strcmp(argv[1], "2") == 0))
        return service_privilege_operations(static_cast<uint64_t>(argv[1][0] - '0'), 100);

    if (argc == 2 && std::strcmp(argv[0], "delegation-check") == 0 &&
        (std::strcmp(argv[1], "1") == 0 || std::strcmp(argv[1], "2") == 0 || std::strcmp(argv[1], "3") == 0))
        return service_privilege_operations(static_cast<uint64_t>(argv[1][0] - '0'));

    if (argc == 1 && std::strcmp(argv[0], "service-policy") == 0) {
        Checks checks;
        checks.expect(user_ids(100, 100, 100) && group_ids(100, 100, 100), "service exec retains only declared IDs");
        checks.expect(getgroups(0, nullptr) == 0, "service has no inherited supplementary groups");
        checks.expect(umask(0077) == 0077, "service retains declared creation mask");
        checks.expect(restriction_call(CredentialControl::GET_NO_NEW_PRIVILEGES) == 1, "service retains no-new-privileges");
        checks.expect(restriction_call(CredentialControl::GET_AUTHORITY_CEILING) == 0, "service retains zero authority ceiling");
        checks.expect(restriction_call(CredentialControl::GET_SERVICE_PRIVILEGES) == ker::abi::process::SERVICE_PRIVILEGES_LOCKED &&
                          restriction_call(CredentialControl::SET_SERVICE_PRIVILEGES, 1) == -EPERM,
                      "init sealed empty service grant before exec");
        checks.expect(seteuid(0) == -1 && errno == EPERM, "service cannot restore effective root");
        checks.expect(setuid(0) == -1 && errno == EPERM, "service cannot restore root user IDs");
        checks.expect(setgid(0) == -1 && errno == EPERM, "service cannot restore root group IDs");
        gid_t const ROOT_GROUP = 0;
        checks.expect(setgroups(1, &ROOT_GROUP) == -1 && errno == EPERM, "service cannot add privileged group membership");
        checks.expect(restriction_call(CredentialControl::DROP_AUTHORITY_CEILING, 1) == -EPERM,
                      "service cannot restore discarded authority");
        checks.expect(restriction_call(CredentialControl::SET_NO_NEW_PRIVILEGES, 0) == -EINVAL, "service cannot clear no-new-privileges");
        checks.expect(kill(1, 0) == -1 && errno == EPERM, "service cannot control init");
        checks.expect(mount("none", "/tmp", "tmpfs", 0, nullptr) == -1 && errno == EPERM, "service cannot mount");
        int const KEY = open("/etc/dropbear/dropbear_rsa_host_key", O_RDONLY | O_CLOEXEC);
        checks.expect(KEY == -1 && errno == EACCES, "service cannot read host private key");
        if (KEY >= 0) close(KEY);
        checks.expect(user_ids(100, 100, 100) && group_ids(100, 100, 100) && getgroups(0, nullptr) == 0 &&
                          restriction_call(CredentialControl::GET_NO_NEW_PRIVILEGES) == 1 &&
                          restriction_call(CredentialControl::GET_AUTHORITY_CEILING) == 0,
                      "denied operations preserve service identity and restrictions");
        std::printf("service identity policy: ");
        return checks.finish();
    }
    if (argc == 1 && std::strcmp(argv[0], "exec-check") == 0) {
        Checks checks;
        checks.expect(user_ids(0, 1000, 1000), "ordinary exec refreshes saved user ID");
        checks.expect(group_ids(2000, 2001, 2001), "ordinary exec refreshes saved group ID");
        return checks.finish();
    }
    if ((argc == 1 || argc == 2) && std::strcmp(argv[0], "restriction-check") == 0) {
        Checks checks;
        checks.expect(user_ids(0, 0, 0) && restriction_call(CredentialControl::GET_NO_NEW_PRIVILEGES) == 1 &&
                          restriction_call(CredentialControl::GET_AUTHORITY_CEILING) == 0,
                      "exec preserves both irreversible process restrictions");
        checks.expect(restriction_call(CredentialControl::DROP_AUTHORITY_CEILING, 1) == -EPERM, "exec cannot restore discarded authority");
        if (argc == 2) {
            std::array<char, 256> hostname{};
            checks.expect(gethostname(hostname.data(), hostname.size()) == 0 && std::strcmp(hostname.data(), argv[1]) == 0,
                          "optional placement retains the original node");
        }
        return checks.finish();
    }
    if (argc == 2 && std::strcmp(argv[0], "setid-restriction-check") == 0) {
        Checks checks;
        bool const NNP = std::strcmp(argv[1], "nnp") == 0;
        bool const ORDINARY = std::strcmp(argv[1], "ordinary") == 0;
        checks.expect(user_ids(1000, NNP ? 1000 : 0, NNP ? 1000 : 0) && group_ids(2000, NNP ? 2000 : 0, NNP ? 2000 : 0),
                      "set-ID exec changes effective/saved IDs only without no-new-privileges");
        checks.expect(restriction_call(CredentialControl::GET_NO_NEW_PRIVILEGES) == (NNP ? 1 : 0) &&
                          restriction_call(CredentialControl::GET_AUTHORITY_CEILING) ==
                              static_cast<int64_t>(ORDINARY ? ker::abi::process::CREDENTIAL_AUTHORITY_ALL : 0),
                      "set-ID exec preserves the inherited ceiling and no-new-privileges");
        if (!ORDINARY)
            checks.expect(mount("none", "/tmp", "tmpfs", 0, nullptr) == -1 && errno == EPERM,
                          "set-ID exec cannot restore discarded MOUNT authority");
        return checks.finish();
    }
    if (argc != 0 || geteuid() != 0) {
        std::fprintf(stderr, "usage: testprog credentials (run as root in a disposable VM)\n");
        return 2;
    }
    Checks checks;
    for (unsigned test = 0; test < 66; ++test) {
        std::printf("credentials: subprocess=%u begins\n", test);
        std::fflush(nullptr);
        pid_t const CHILD = fork();
        if (CHILD == 0) {
            alarm(30);
            int result = 1;
            if (test == 0) result = transitions();
            if (test == 1) result = threads(false);
            if (test == 2) result = threads(true);
            if (test == 3) result = ordinary_exec();
            if (test == 4) result = controls();
            if (test == 5) result = signal_policy();
            if (test == 6) result = priority_policy();
            if (test == 7) result = trace_policy();
            if (test == 8) result = trace_registers();
            if (test == 9) result = shared_memory_policy();
            if (test == 10) result = shared_mapping_rights();
            if (test == 11) result = metadata_policy("/tmp");
            if (test == 12) result = metadata_policy("");  // XFS is / after init's pivot_root.
            if (test == 13) result = metadata_snapshot_race("/tmp");
            if (test == 14) result = metadata_snapshot_race("");
            if (test == 15) result = devfs_metadata_policy();
            if (test == 16) result = devfs_metadata_race();
            if (test == 17) result = fat_metadata_policy();
            if (test == 18) result = timestamp_policy("/tmp");
            if (test == 19) result = timestamp_policy("");
            if (test == 20) result = timestamp_policy(nullptr);
            if (test == 21) result = xattr_policy();
            if (test == 22) result = file_open_policy("/tmp");
            if (test == 23) result = file_open_policy("");
            if (test == 24) result = fat_open_policy();
            if (test == 25) result = devfs_open_policy();
            if (test == 26) result = procfs_global_policy();
            if (test == 27) result = procfs_fd_policy();
            if (test == 28) result = procfs_private_policy();
            if (test == 29) result = procfs_public_policy();
            if (test == 30) result = procfs_wki_identity_policy();
            if (test == 31) result = coredump_output_policy();
            if (test == 32) result = journal_read_policy();
            if (test == 33) result = removal_policy("/tmp");
            if (test == 34) result = removal_policy("");
            if (test == 35) result = fat_removal_policy();
            if (test == 36) result = directory_identity_policy("/tmp");
            if (test == 37) result = directory_identity_policy("");
            if (test == 38) result = directory_creation_policy("/tmp");
            if (test == 39) result = directory_creation_policy("");
            if (test == 40) result = symlink_creation_policy("/tmp");
            if (test == 41) result = symlink_creation_policy("");
            if (test == 42) result = hardlink_policy("/tmp");
            if (test == 43) result = hardlink_policy("");
            if (test == 44) result = rename_policy("/tmp");
            if (test == 45) result = rename_policy("");
            if (test == 46) result = fat_rename_policy();
            if (test == 47) result = directory_search_policy("/tmp");
            if (test == 48) result = directory_search_policy("");
            if (test == 49) result = original_component_policy("/tmp");
            if (test == 50) result = original_component_policy("");
            if (test == 51) result = retained_cwd_policy("/tmp");
            if (test == 52) result = retained_cwd_policy("");
            if (test == 53) result = mount_administration_policy();
            if (test == 54) result = mount_target_policy("/tmp");
            if (test == 55) result = mount_target_policy("");
            if (test == 56) result = mount_credential_race("/tmp");
            if (test == 57) result = mount_credential_race("");
            if (test == 58) result = privilege_restrictions();
            if (test == 59) result = setid_exec_restrictions("/tmp");
            if (test == 60) result = setid_exec_restrictions("");
            if (test == 61) result = network_administration_policy(false);
            if (test == 62) result = network_administration_policy(true);
            if (test >= 63 && test <= 65) result = service_privilege_policy(test - 62);
            std::fflush(nullptr);
            _exit(result);
        }
        checks.expect(wait_ok(CHILD), "credential subprocess");
    }
    return checks.finish();
}
