// Compiled actual controller/owner/stamp/observer definitions. Raw wire and
// WOS-only authority are explicit Host inputs; no117-work/WOS/runtime claim.
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "uma_reference_arm.hpp"
#include "uma_reference_catalog.hpp"
#include "uma_reference_owner.hpp"

extern auto uma_controller_main(int, char**) -> int;
extern auto uma_witness_main(int, char**) -> int;
extern auto uma_arm_host_snapshot(ker::abi::init_control::StatusSnapshot&) -> int64_t;
namespace {
namespace arm = uma_reference::arm;
unsigned checks{}, failures{}, consuming{}, interrupted{};
bool after_consume{}, cancel_warm{}, eintr{}, storm{};
const char* current_phase{};
std::string executable, root, nonce, phase_arg, stop_arg;
std::array<char*, 8> child_args{};
void check(bool okay, const char* label) {
    ++checks;
    if (!okay) {
        ++failures;
        std::fprintf(stderr, "FAIL %s\n", label);
    }
}
auto present(const std::string& path) -> bool {
    struct stat s{};
    return ::lstat(path.c_str(), &s) == 0;
}
void file(const std::string& path, const std::string& data, mode_t mode = 0600) {
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, mode);
    if (fd < 0 || ::write(fd, data.data(), data.size()) != ssize_t(data.size()) || ::close(fd)) ::_exit(98);
}
auto suite(int argc, char** argv) -> int {
    if (argc != 7) return 97;
    std::string r = argv[2], phase = argv[3], nonce = argv[4], deadline = argv[5];
    bool bad = std::strcmp(argv[6], "bad-warm") == 0 && phase == "warmup";
    std::string summary;
    std::printf("WOS userland suite\n");
    uint64_t res = 1;
    for (size_t i = 0; i < uma_reference::CASES.size(); ++i) {
        std::string name(uma_reference::CASES[i]);
        uint64_t start{};
        if (!arm::now(start)) return 97;
        uint64_t child = 100000 + i, helper = 200000 + i;
        auto prefix = " v=1 nonce=" + nonce + " phase=" + phase + " case=" + name;
        auto path = r + "/" + phase + "/timing/" + name;
        file(path + ".begin", "UMA_BEGIN" + prefix + " helper=" + std::to_string(helper) + " child=" + std::to_string(child) +
                                  " start=" + std::to_string(start) + " res=" + std::to_string(res) + " deadline=" + deadline + "\n");
        uint64_t end{};
        do {
            if (!arm::now(end)) return 97;
        } while (end <= start);
        file(path + ".end", "UMA_END" + prefix + " helper=" + std::to_string(helper + 1000) + " child=" + std::to_string(child) +
                                " start=" + std::to_string(start) + " end=" + std::to_string(end) +
                                " res=1 body=0 effective=0 complete=1\n");
        file(path + ".parent", "UMA_PARENT" + prefix + " helper=" + std::to_string(helper + 2000) + " child=" + std::to_string(child) +
                                   " suite=" + std::to_string(::getpid()) + "\n");
        file(path + ".wait", "UMA_WAIT" + prefix + " child=" + std::to_string(child) + " suite=" + std::to_string(::getpid()) +
                                 " marker=0 decoded=0 timeout=0 joined=1\n");
        std::string log = "Host transport fixture, no actual benchmark work\n";
        if (name == "make_large_file")
            log = bad ? "fallback to different work\n"
                      : "created 67108864 bytes at /var/lib/uma-reference/" + phase + "/work/large-input.bin using /dev/zero\n";
        file(r + "/" + phase + "/logs/" + name + ".log", log, 0644);
        summary += name + "\tPASS\t0s\n";
        std::printf("PASS %s (0s)\n", name.c_str());
    }
    file(r + "/" + phase + "/summary.tsv", summary, 0644);
    std::printf("=== WOS USERLAND SUITE SUMMARY ===\n%sPASS=117 FAIL=0 SKIP=0\n", summary.c_str());
    return 0;
}
void validators() {
    arm::Profile p{};
    p.pid = 100;
    p.parent = 1;
    p.tid = 100;
    p.cpus = 8;
    p.affinity = 255;
    p.authority = 65535;
    p.target_flags = 2;
    p.empty_target = true;
    check(arm::profile_valid(p, 1), "actual profile validator positive");
    for (unsigned index = 0; index < 21; ++index) {
        auto x = p;
        if (index < 3)
            x.uid[index] = 1;
        else if (index < 6)
            x.gid[index - 3] = 1;
        else
            switch (index) {
                case 6:
                    x.groups = 1;
                    break;
                case 7:
                    x.pid = 1;
                    break;
                case 8:
                    x.parent = 2;
                    break;
                case 9:
                    x.tid = 99;
                    break;
                case 10:
                    x.cpus = 4;
                    break;
                case 11:
                    x.affinity = 1;
                    break;
                case 12:
                    x.authority = 16;
                    break;
                case 13:
                    x.nnp = 1;
                    break;
                case 14:
                    x.privileges = 256;
                    break;
                case 15:
                    x.priority = 1;
                    break;
                case 16:
                    x.target_result = -1;
                    break;
                case 17:
                    x.target_flags = 0;
                    break;
                case 18:
                    x.empty_target = false;
                    break;
                case 19:
                    x.pid = UINT64_MAX;
                    break;
                case 20:
                    x.parent = 0;
                    break;
            }
        check(!arm::profile_valid(x, 1), "each actual profile field refusal");
    }
    arm::init::StatusSnapshot s{};
    arm::init::ServiceStatus selected{};
    check(uma_arm_host_snapshot(s) == 0 && arm::snapshot_valid(s, selected), "actual whole8 wire positive");
    for (unsigned i = 0; i < 10; ++i) {
        auto x = s;
        switch (i) {
            case 0:
                x.service_count = 7;
                break;
            case 1:
                x.reserved[0] = 1;
                break;
            case 2:
                x.flags = 1;
                break;
            case 3:
                x.services[15].flags = 1;
                break;
            case 4:
                x.services[0].service[63] = 1;
                break;
            case 5:
                x.services[0].history[3].reserved0 = 1;
                break;
            case 6:
                std::strcpy(x.services[1].service, x.services[0].service);
                break;
            case 7:
                x.services[0].state = arm::init::ServiceState::UNKNOWN;
                break;
            case 8:
                x.services[0].readiness = arm::init::ReadinessState::UNKNOWN;
                break;
            case 9:
                x.services[0].history_count = 5;
                break;
        }
        check(!arm::snapshot_valid(x, selected), "actual wire malformed field refused");
    }
    uint64_t deadline{};
    check(arm::running(s, ::getpid(), deadline), "actual EXEC deadline/history positive");
    auto x = s;
    x.services[6].deadline_mono_ns += 1;
    check(!arm::running(x, ::getpid(), deadline), "no invented refreshed deadline");
    x = s;
    x.services[6].history[0].reason = arm::init::TransitionReason::READINESS;
    check(!arm::running(x, ::getpid(), deadline), "must be actual EXEC/SPAWN transition");
    arm::Completion original{};
    std::strcpy(original.nonce.data(), "0123456789abcdef0123456789abcdef");
    original.controller = 100;
    original.warm = 101;
    original.measure = 102;
    original.deadline = 1000 * arm::SECOND;
    original.work_stop = 970 * arm::SECOND;
    original.phases[0] = {1, 2, 1, 100, 100, 117, 11};
    original.phases[1] = {3, 4, 1, 100, 100, 117, 11};
    std::array<char, arm::COMPLETION_LIMIT> text{};
    auto n = arm::format_completion(text.data(), text.size(), original);
    arm::Completion out{};
    check(n > 0 && arm::parse_completion(text.data(), size_t(n), original.nonce.data(), out), "actual completion format/parser join");
    text[size_t(n) - 1] = '\r';
    check(!arm::parse_completion(text.data(), size_t(n), original.nonce.data(), out), "raw CR refusal");
}
}  // namespace
auto uma_owner_host_waitpid(pid_t pid, int* raw, int options) -> pid_t {
    if (after_consume) {
        check(false, "no postconsumption wait");
        errno = ECHILD;
        return -1;
    }
    if (options & WNOWAIT) {
        siginfo_t info{};
        if (::waitid(P_PID, id_t(pid), &info, WEXITED | WNOHANG | WNOWAIT)) return -1;
        if (!info.si_pid) return 0;
        *raw = info.si_code == CLD_EXITED ? info.si_status << 8 : info.si_status | (info.si_code == CLD_DUMPED ? 0x80 : 0);
        return info.si_pid;
    }
    ++consuming;
    if ((eintr && interrupted++ < 3) || storm) {
        errno = EINTR;
        return -1;
    }
    auto result = ::waitpid(pid, raw, options);
    if (result == pid) after_consume = true;
    return result;
}
auto uma_owner_host_kill(pid_t target, int sig) -> int {
    check(!after_consume, "no postconsumption signals");
    return ::kill(target, sig);
}
void uma_owner_host_admitted() {}
void uma_owner_host_after_consume() {
    if (cancel_warm && current_phase && !std::strcmp(current_phase, "warmup")) ::kill(::getpid(), SIGTERM);
}
void uma_arm_host_request(uma_reference::PhaseRequest& r) {
    // Setup happens before this phase's actual fork. Previous child was already
    // consumed. No policy/result state is inferred by these Host request inputs.
    after_consume = false;
    current_phase = r.phase;
    phase_arg = r.phase;
    stop_arg = std::to_string(r.work_stop_ns);
    const char* mode = std::getenv("UMA_HOST_MODE");
    child_args = {executable.data(),
                  const_cast<char*>("--suite"),
                  root.data(),
                  phase_arg.data(),
                  nonce.data(),
                  stop_arg.data(),
                  const_cast<char*>(mode ? mode : "positive"),
                  nullptr};
    r.host_root = root.c_str();
    r.host_argv = child_args.data();
    r.host_grace_ns = 50'000'000;
}
auto main(int argc, char** argv) -> int {
    if (argc >= 2 && !std::strcmp(argv[1], "--suite")) return suite(argc, argv);
    if (argc == 3 && !std::strcmp(argv[1], "--observe-only")) {
        ::setenv("WOS_UMA_TEST_ROOT", argv[2], 1);
        char* observe[]{argv[0], const_cast<char*>("--observe"), const_cast<char*>("--nonce"),
                        const_cast<char*>("0123456789abcdef0123456789abcdef"), nullptr};
        return uma_witness_main(4, observe);
    }
    if (argc != 3) return 2;
    executable = argv[0];
    root = argv[2];
    nonce = "0123456789abcdef0123456789abcdef";
    const char* mode = argv[1];
    if (!std::strcmp(mode, "validators")) {
        validators();
    } else {
        ::setenv("WOS_UMA_TEST_ROOT", root.c_str(), 1);
        ::setenv("USER", "root", 1);
        ::setenv("HOME", "/root", 1);
        ::setenv("HOSTNAME", "uma-reference-vm0", 1);
        ::setenv("UMA_HOST_MODE", mode, 1);
        std::string clock = root + ".clock";
        ::setenv("WOS_UMA_CLOCK_SOURCE", clock.c_str(), 1);
        cancel_warm = !std::strcmp(mode, "cancel");
        eintr = !std::strcmp(mode, "eintr");
        storm = !std::strcmp(mode, "storm");
        if (!std::strcmp(mode, "child-refuse")) ::setenv("UMA_HOST_REJECT_CHILD", "1", 1);
        char* args[]{argv[0], const_cast<char*>("--run"), const_cast<char*>("--nonce"), nonce.data(), nullptr};
        int result = uma_controller_main(4, args);
        int wanted = (!std::strcmp(mode, "cancel") || !std::strcmp(mode, "child-refuse") || storm) ? 73
                     : !std::strcmp(mode, "bad-warm")                                              ? 74
                                                                                                   : 0;
        check(result == wanted, "actual controller exact terminal result");
        if (wanted) {
            check(!present(root + "/measure/phase.log"), "actual failure cannot start measure");
            check(!present(root + "/completion.txt"), "failure cannot publish complete arm");
        } else {
            check(present(root + "/completion.txt"), "actual both-phase completion saved");
            if (eintr) check(interrupted >= 3 && consuming == 5, "actual consuming EINTR retries bounded and then succeeds");
            ::setenv("UMA_HOST_CONSUMED", "1", 1);
            if (!std::strcmp(mode, "bad-init")) ::setenv("UMA_HOST_BAD_INIT", "1", 1);
            pid_t observer = ::fork();
            if (observer == 0) {
                ::execl(argv[0], argv[0], "--observe-only", root.c_str(), static_cast<char*>(nullptr));
                ::_exit(96);
            }
            uint64_t limit{};
            if (!arm::now(limit)) return 95;
            limit += 2 * arm::SECOND;
            int raw = -1;
            pid_t got = 0;
            while (observer > 1 && got == 0) {
                got = ::waitpid(observer, &raw, WNOHANG);
                if (got < 0 && errno == EINTR) {
                    got = 0;
                    continue;
                }
                uint64_t n{};
                if (!arm::now(n) || n >= limit) break;
                if (!got) ::usleep(1000);
            }
            int observed = got == observer && WIFEXITED(raw) ? WEXITSTATUS(raw) : -1;
            check(observed == (!std::strcmp(mode, "bad-init") ? 82 : 0), "actual independent observer enforces consumed READY");
            check(present(root + "/host-poweroff.txt") == (std::strcmp(mode, "bad-init") != 0),
                  "only validated observer requests Host-only poweroff seam");
        }
    }
    std::printf("ARM_CASE mode=%s checks=%u failures=%u\n", mode, checks, failures);
    return failures ? 1 : 0;
}
