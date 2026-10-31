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
#include <limits>
#include <string>

#include "service_manifest.h"
#include "supervisor_model.h"
#include "uma_reference_arm.hpp"
#include "uma_reference_catalog.hpp"
#include "uma_reference_owner.hpp"

extern auto uma_controller_main(int, char**) -> int;
extern auto uma_witness_main(int, char**) -> int;
extern auto uma_arm_host_snapshot(ker::abi::init_control::StatusSnapshot&) -> int64_t;
extern void uma_arm_host_complete(ker::abi::init_control::StatusSnapshot&, pid_t);
extern void uma_arm_host_state(ker::abi::init_control::StatusSnapshot&);
extern void uma_arm_host_pending(ker::abi::init_control::StatusSnapshot&);
extern void uma_arm_host_witness_complete(ker::abi::init_control::StatusSnapshot&, pid_t);
extern void uma_arm_host_final_pending(ker::abi::init_control::StatusSnapshot&);
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
auto capture_owner_failure(const uma_reference::PhaseRequest& request, const uma_reference::PhaseOutcome& outcome) -> std::string {
    std::string path = root + ".diagnostics";
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    int saved = ::dup(STDERR_FILENO);
    if (fd < 0 || saved < 0 || std::fflush(stderr) || ::dup2(fd, STDERR_FILENO) != STDERR_FILENO) ::_exit(98);
    errno = EDOM;
    uma_reference::report_owner_failure(request, outcome);
    bool const ERRNO_PRESERVED = errno == EDOM;
    if (std::fflush(stderr) || ::dup2(saved, STDERR_FILENO) != STDERR_FILENO || ::close(saved)) ::_exit(98);
    check(ERRNO_PRESERVED, "actual failure reporter preserves caller errno");
    struct stat status{};
    std::array<char, 4096> bytes{};
    if (::fstat(fd, &status) || status.st_size < 0 || uint64_t(status.st_size) >= bytes.size() || ::lseek(fd, 0, SEEK_SET) != 0)
        ::_exit(98);
    ssize_t count = ::read(fd, bytes.data(), bytes.size());
    if (count != status.st_size || ::close(fd) || ::unlink(path.c_str())) ::_exit(98);
    std::string result(bytes.data(), size_t(count));
    size_t at{};
    unsigned lines{};
    bool bounded = true;
    while (at < result.size()) {
        size_t end = result.find('\n', at);
        if (end == std::string::npos) {
            bounded = false;
            break;
        }
        bounded = bounded && end - at + 1 < 512;
        ++lines;
        at = end + 1;
    }
    check(bounded && lines == 3, "actual failure reporter emits exactly three journal-bounded lines");
    return result;
}
void owner_diagnostics() {
    using namespace uma_reference;
    PhaseRequest request{};
    request.nonce = "ffffffffffffffffffffffffffffffffSENTINEL_NONCE";
    request.phase = "measureSENTINEL_PHASE";
    PhaseOutcome outcome{};
    outcome.failure = static_cast<OwnerFailure>(UINT8_MAX);
    outcome.child = std::numeric_limits<pid_t>::min();
    outcome.observed_raw = outcome.consumed_raw = outcome.system_error = std::numeric_limits<int>::min();
    outcome.nonconsuming_completions = outcome.consuming_completions = outcome.term_calls = outcome.kill_calls = UINT32_MAX;
    outcome.admitted = outcome.consumed = outcome.raw_phase_zero = true;
    auto& detail = outcome.inspection;
    detail.check = static_cast<InspectionCheck>(UINT8_MAX);
    detail.entry_truncated = detail.syscall_error_valid = detail.metadata_valid = true;
    detail.directory_fd = detail.system_error = detail.syscall_result = std::numeric_limits<int>::min();
    detail.depth = UINT32_MAX;
    detail.observed = detail.limit = detail.device = detail.inode = detail.mode = detail.uid = detail.gid = detail.links = UINT64_MAX;
    detail.size = INT64_MIN;
    constexpr std::array<unsigned char, 5> RAW = {0x0a, 0x0d, 0x1b, 0x80, 0xff};
    for (size_t i = 0; i < detail.entry.size() - 1; ++i) detail.entry[i] = static_cast<char>(RAW[i % RAW.size()]);
    std::string expected_hex;
    for (unsigned i = 0; i < 25; ++i) expected_hex += "0a0d1b80ff";
    expected_hex += "0a0d";
    auto text = capture_owner_failure(request, outcome);
    check(text.find("SENTINEL") == std::string::npos &&
              text.find("nonce=ffffffffffffffffffffffffffffffff phase=measure ") != std::string::npos,
          "actual failure reporter bounds nonce and phase widths");
    check(text.find("failure=255 check=255 errno_valid=1 error=" + std::to_string(std::numeric_limits<int>::min())) != std::string::npos &&
              text.find("value=18446744073709551615 limit=18446744073709551615 metadata_valid=1") != std::string::npos &&
              text.find("size=-9223372036854775808\n") != std::string::npos,
          "actual failure reporter retains maximum unsigned and minimum signed scalar values");
    check(text.find("hex=" + expected_hex + " truncated=1\n") != std::string::npos && text.find('\r') == std::string::npos &&
              text.find('\x1b') == std::string::npos && text.find('\x80') == std::string::npos && text.find('\xff') == std::string::npos,
          "actual failure reporter hex escapes all127 basename bytes without line injection");
    detail.entry.back() = 'x';
    detail.entry_truncated = false;
    text = capture_owner_failure(request, outcome);
    check(text.find("hex=" + expected_hex + " truncated=1\n") != std::string::npos,
          "actual failure reporter bounds an unterminated entry and exposes truncation");
    text = capture_owner_failure({}, {});
    check(text.find("failure=0 check=0 errno_valid=0 error=0 old_error=0") != std::string::npos &&
              text.find("fd=-1 depth=0 result=0 value=0 limit=0 metadata_valid=0") != std::string::npos &&
              text.find("hex= truncated=0\n") != std::string::npos,
          "actual failure reporter distinguishes absent inspection and metadata evidence");
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
    check(uma_arm_host_snapshot(s) == 0 && arm::snapshot_valid(s, selected), "actual whole9 wire positive");
    for (unsigned i = 0; i < 10; ++i) {
        auto x = s;
        switch (i) {
            case 0:
                x.service_count = 8;
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
    x.services[6].deadline_mono_ns = x.services[6].state_since_mono_ns + 600 * arm::SECOND;
    uint64_t refused = 0x741;
    check(!arm::running(x, ::getpid(), refused) && refused == 0x741, "old600 authority refused without changing admitted deadline");
    x = s;
    x.services[6].deadline_mono_ns += 1;
    check(!arm::running(x, ::getpid(), deadline), "no invented refreshed deadline");
    x = s;
    --x.services[6].state_since_mono_ns;
    --x.services[6].deadline_mono_ns;
    check(!arm::running(x, ::getpid(), deadline), "same750 span cannot renew actual EXEC history");
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
void consumed_rows() {
    arm::init::StatusSnapshot s{};
    check(uma_arm_host_snapshot(s) == 0, "actual composed initial snapshot");
    uma_arm_host_complete(s, ::getpid());
    check(s.state == arm::init::SupervisorState::STARTING, "actual production classifier sees unfinished observer");
    check(arm::consumed(s, ::getpid()), "actual consumed controller with live observer positive");
    auto pending = s;
    uma_arm_host_pending(pending);
    check(pending.state == arm::init::SupervisorState::STARTING && arm::awaiting_exec(pending, ::getpid()) &&
              !arm::consumed(pending, ::getpid()),
          "actual own pending EXEC is polling only");
    check(!arm::awaiting_exec(s, ::getpid()), "final EXEC row is not pending");
    auto invalid = pending;
    invalid.services[7].history[0].reason = arm::init::TransitionReason::SPAWN;
    check(!arm::awaiting_exec(invalid, ::getpid()), "pending requires real dependency transition");
    invalid = pending;
    ++invalid.services[7].deadline_mono_ns;
    check(!arm::awaiting_exec(invalid, ::getpid()), "pending cannot refresh original deadline");
    check(!arm::awaiting_exec(pending, ::getpid() + 1), "pending requires actual observer PID");
    auto keygen = s;
    keygen.services[1].flags = 1;
    keygen.services[1].generation = 1;
    keygen.services[1].history[0].from_state = arm::init::ServiceState::STOPPING;
    keygen.services[1].history[0].reason = arm::init::TransitionReason::EXIT;
    check(arm::consumed(keygen, ::getpid()), "actual consumed keygen alternative positive");
    for (unsigned i = 0; i < 32; ++i) {
        auto x = s;
        auto& observer = x.services[7];
        auto& controller = x.services[6];
        switch (i) {
            case 0:
                x.state = arm::init::SupervisorState::RUNNING;
                break;
            case 1:
                x.state = arm::init::SupervisorState::STOPPING;
                break;
            case 2:
                x.state = arm::init::SupervisorState::FAILED;
                break;
            case 3:
                ++observer.pid;
                break;
            case 4:
                ++observer.pgid;
                break;
            case 5:
                observer.generation = 2;
                break;
            case 6:
                observer.restart_count = 1;
                break;
            case 7:
                observer.state = arm::init::ServiceState::STARTING;
                break;
            case 8:
                observer.readiness = arm::init::ReadinessState::READY;
                break;
            case 9:
                observer.flags = 7;
                break;
            case 10:
                ++observer.deadline_mono_ns;
                break;
            case 11:
                x.published_mono_ns = observer.deadline_mono_ns;
                break;
            case 12:
                observer.state_since_mono_ns = controller.state_since_mono_ns - 1;
                break;
            case 13:
                observer.history_count = 0;
                std::memset(observer.history, 0, sizeof(observer.history));
                break;
            case 14:
                observer.history[0].from_state = arm::init::ServiceState::RUNNING;
                break;
            case 15:
                observer.history[0].reason = arm::init::TransitionReason::READINESS;
                break;
            case 16:
                --observer.history[0].timestamp_mono_ns;
                break;
            case 17:
                x.services[0].state = arm::init::ServiceState::WAITING;
                x.services[0].readiness = arm::init::ReadinessState::NOT_READY;
                uma_arm_host_state(x);
                break;
            case 18:
                x.services[0].state = arm::init::ServiceState::FAILED;
                break;
            case 19:
                x.services[0].restart_count = 1;
                break;
            case 20:
                x.services[0].deadline_mono_ns = 1;
                break;
            case 21:
                x.services[1].state = arm::init::ServiceState::RUNNING;
                x.services[1].readiness = arm::init::ReadinessState::NOT_READY;
                break;
            case 22:
                x.services[1].generation = 2;
                break;
            case 23:
                x.services[5].state = arm::init::ServiceState::RUNNING;
                break;
            case 24:
                controller.last_wait_status = 1;
                break;
            case 25:
                controller.pid = ::getpid();
                break;
            case 26:
                controller.history[0].reason = arm::init::TransitionReason::SPAWN;
                break;
            case 27:
                controller.deadline_mono_ns = 1;
                break;
            case 28:
                controller.generation = 2;
                break;
            case 29:
                x.services[0].history[0].reason = arm::init::TransitionReason::TIMEOUT;
                break;
            case 30:
                observer.last_error = 1;
                break;
            case 31:
                observer.history[0].timestamp_mono_ns = x.published_mono_ns + 1;
                break;
        }
        check(!arm::consumed(x, ::getpid()), "actual final service authority rejects mutation");
    }
    check(!arm::consumed(s, ::getpid() + 1), "actual caller PID must own observer row");
}

void finalized_rows() {
    arm::init::StatusSnapshot earlier{};
    check(uma_arm_host_snapshot(earlier) == 0, "actual nine-row initial snapshot");
    uma_arm_host_complete(earlier, ::getpid());
    auto final = earlier;
    ++final.published_mono_ns;
    pid_t const FINALIZER = ::getpid() + 1;
    uma_arm_host_witness_complete(final, FINALIZER);
    uint64_t const BOUND = earlier.services[7].deadline_mono_ns - 1;
    check(arm::witness_consumed(final, earlier, FINALIZER, BOUND), "two consumed raw0 rows with live finalizer positive");
    check(final.services[8].state == arm::init::ServiceState::RUNNING && arm::witness_consumed(final, earlier, FINALIZER, BOUND),
          "finalizer own consumption is not recursive proof");
    auto pending = final;
    uma_arm_host_final_pending(pending);
    check(arm::witness_awaiting_exec(pending, earlier, FINALIZER, BOUND) && !arm::witness_consumed(pending, earlier, FINALIZER, BOUND),
          "pending finalizer EXEC is polling only");
    for (unsigned i = 0; i < 17; ++i) {
        auto bad = final;
        switch (i) {
            case 0:
                bad.services[7].state = arm::init::ServiceState::RUNNING;
                break;
            case 1:
                bad.services[7].readiness = arm::init::ReadinessState::NOT_READY;
                break;
            case 2:
                bad.services[7].last_wait_status = 1;
                break;
            case 3:
                bad.services[7].generation = 2;
                break;
            case 4:
                bad.services[7].restart_count = 1;
                break;
            case 5:
                bad.services[7].pid = ::getpid();
                break;
            case 6:
                bad.services[7].deadline_mono_ns = BOUND;
                break;
            case 7:
                bad.services[7].history[0].reason = arm::init::TransitionReason::SPAWN;
                break;
            case 8:
                bad.services[8].pid = FINALIZER + 1;
                break;
            case 9:
                bad.services[8].generation = 2;
                break;
            case 10:
                bad.services[8].deadline_mono_ns++;
                break;
            case 11:
                bad.services[6].last_wait_status = 1;
                break;
            case 12:
                bad.services[0].pid++;
                break;
            case 13:
                bad.published_mono_ns = BOUND;
                break;
            case 14:
                bad.sequence = earlier.sequence;
                break;
            case 15:
                bad.services[8].history[0].from_state = arm::init::ServiceState::WAITING;
                break;
            case 16:
                bad.services[8].history[0].timestamp_mono_ns++;
                break;
        }
        check(!arm::witness_consumed(bad, earlier, FINALIZER, BOUND), "actual dual-consumed validator rejects mutation");
    }
    check(!arm::witness_consumed(final, earlier, FINALIZER, earlier.services[7].deadline_mono_ns + 1),
          "original bound cannot exceed witness service deadline");
    check(!arm::witness_consumed(final, earlier, FINALIZER + 1, BOUND), "finalizer caller identity must match");
}
void dependency_gate() {
    namespace model = wos::init;
    std::array<char, model::MAX_MANIFEST_BYTES + 1> bytes{};
    std::string path = root + ".services";
    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    ssize_t n = fd < 0 ? -1 : ::read(fd, bytes.data(), bytes.size());
    if (fd >= 0) ::close(fd);
    model::ServiceManifest manifest{};
    model::ServiceTopology topology{};
    bool const PARSED = n > 0 && size_t(n) < bytes.size() &&
                        bool(model::parse_service_manifest(std::span(bytes.data(), size_t(n)), manifest)) &&
                        bool(model::validate_service_manifest(manifest, topology));
    check(PARSED && manifest.service_count == 9, "actual corrected nine-service manifest parses");
    if (!PARSED || manifest.service_count != 9) return;
    model::SupervisorModel state{};
    std::array<model::EnablementResult, model::MAX_SERVICES> enabled{};
    enabled.fill(model::EnablementResult::CONDITION_MET);
    check(model::initialize_supervisor(state, manifest, topology, std::span(enabled.data(), manifest.service_count), 1) ==
              model::SupervisorResult::OK,
          "actual supervisor initializes corrected manifest");
    unsigned controller = 99, witness = 99, finalizer = 99;
    for (unsigned i = 0; i < manifest.service_count; ++i) {
        if (manifest.services[i].name.view() == arm::SERVICE) controller = i;
        if (manifest.services[i].name.view() == arm::OBSERVER) witness = i;
        if (manifest.services[i].name.view() == arm::FINALIZER) finalizer = i;
        state.services[i].state = model::ServiceState::READY;
    }
    check(controller < 9 && witness < 9 && finalizer < 9 && manifest.services[controller].readiness_timeout_ms == 750000 &&
              manifest.services[witness].readiness_timeout_ms == 30000 && manifest.services[finalizer].readiness_timeout_ms == 30000,
          "actual manifest750 controller with unchanged30 witness/finalizer");
    check(witness < 9 && finalizer < 9 && (manifest.services[finalizer].dependency_mask & (1U << witness)),
          "actual finalizer depends on original witness");
    if (witness >= 9 || finalizer >= 9) return;
    auto& original = state.services[witness];
    original.state = model::ServiceState::RUNNING;
    check(!model::supervisor_dependencies_satisfied(state, uint8_t(finalizer)), "live witness does not release finalizer");
    original.state = model::ServiceState::EXITED;
    original.completion_success = false;
    original.quiesced = true;
    check(!model::supervisor_dependencies_satisfied(state, uint8_t(finalizer)), "nonzero witness does not release finalizer");
    original.completion_success = true;
    original.quiesced = false;
    check(!model::supervisor_dependencies_satisfied(state, uint8_t(finalizer)), "reaped but unquiesced witness does not release finalizer");
    original.quiesced = true;
    check(model::supervisor_dependencies_satisfied(state, uint8_t(finalizer)), "consumed successful quiescent witness releases finalizer");
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
        ::setenv("UMA_HOST_LATE_EXEC", "1", 1);
        char* observe[]{argv[0], const_cast<char*>("--observe"), const_cast<char*>("--nonce"),
                        const_cast<char*>("0123456789abcdef0123456789abcdef"), nullptr};
        return uma_witness_main(4, observe);
    }
    if (argc == 3 && !std::strcmp(argv[1], "--finalize-only")) {
        ::setenv("WOS_UMA_TEST_ROOT", argv[2], 1);
        ::setenv("UMA_HOST_FINALIZE", "1", 1);
        char* finalize[]{argv[0], const_cast<char*>("--finalize"), const_cast<char*>("--nonce"),
                         const_cast<char*>("0123456789abcdef0123456789abcdef"), nullptr};
        return uma_witness_main(4, finalize);
    }
    if (argc != 3) return 2;
    executable = argv[0];
    root = argv[2];
    nonce = "0123456789abcdef0123456789abcdef";
    const char* mode = argv[1];
    if (!std::strcmp(mode, "diagnostics")) {
        owner_diagnostics();
    } else if (!std::strcmp(mode, "validators")) {
        validators();
        dependency_gate();
    } else if (!std::strcmp(mode, "consumed")) {
        consumed_rows();
        finalized_rows();
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
            if (!std::strcmp(mode, "pending-exec")) ::setenv("UMA_HOST_PENDING_EXEC", "1", 1);
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
            if (!std::strcmp(mode, "pending-exec")) std::fprintf(stderr, "PENDING_EXEC_OBSERVER_EXIT=%d\n", observed);
            check(observed == (!std::strcmp(mode, "bad-init") ? 82 : 0), "actual independent observer enforces consumed READY");
            check(!present(root + "/host-poweroff.txt") && !present(root + "/host-observe-poweroff-attempt.txt"),
                  "actual observer returns0 without any poweroff request");
            if (observed == 0) {
                arm::init::StatusSnapshot earlier{};
                size_t size{};
                std::array<char, 512> window{};
                unsigned long long observer_pid{}, entered{}, bound{};
                std::array<char, 33> window_nonce{};
                bool const WINDOW =
                    arm::read("init-exit.bin", &earlier, sizeof(earlier), size) && size == sizeof(earlier) &&
                    arm::read("witness-window.txt", window.data(), window.size(), size) &&
                    std::sscanf(window.data(), "UMA_WITNESS_WINDOW v=2 nonce=%32[0-9a-f] observer=%llu entered=%llu deadline=%llu",
                                window_nonce.data(), &observer_pid, &entered, &bound) == 4;
                check(WINDOW && observer_pid == uint64_t(observer) && bound == entered + arm::MARGIN &&
                          bound < earlier.services[7].deadline_mono_ns,
                      "original observer entry precedes EXEC and bounds finalizer");
                if (!std::strcmp(mode, "pending-final")) ::setenv("UMA_HOST_PENDING_FINAL", "1", 1);
                if (!std::strcmp(mode, "bad-final")) ::setenv("UMA_HOST_BAD_FINAL", "1", 1);
                pid_t finalizer = ::fork();
                if (finalizer == 0) {
                    ::execl(argv[0], argv[0], "--finalize-only", root.c_str(), static_cast<char*>(nullptr));
                    ::_exit(96);
                }
                uint64_t until{};
                if (!arm::now(until)) return 95;
                until += 2 * arm::SECOND;
                int final_raw = -1;
                pid_t final_got = 0;
                while (finalizer > 1 && final_got == 0) {
                    final_got = ::waitpid(finalizer, &final_raw, WNOHANG);
                    if (final_got < 0 && errno == EINTR) {
                        final_got = 0;
                        continue;
                    }
                    uint64_t tick{};
                    if (!arm::now(tick) || tick >= until) break;
                    if (!final_got) ::usleep(1000);
                }
                int const FINAL_EXIT = final_got == finalizer && WIFEXITED(final_raw) ? WEXITSTATUS(final_raw) : -1;
                if (!std::strcmp(mode, "pending-final")) std::fprintf(stderr, "PENDING_FINALIZER_EXIT=%d\n", FINAL_EXIT);
                bool const EXPECTED = std::strcmp(mode, "bad-final") != 0;
                check(FINAL_EXIT == (EXPECTED ? 0 : 92), "actual independent finalizer validates original witness consumption");
                check(present(root + "/host-poweroff.txt") == EXPECTED && present(root + "/init-final.bin") == EXPECTED &&
                          present(root + "/init-final.txt") == EXPECTED,
                      "only finalizer with saved dual-consumed proof requests poweroff");
            }
        }
    }
    std::printf("ARM_CASE mode=%s checks=%u failures=%u\n", mode, checks, failures);
    return failures ? 1 : 0;
}
