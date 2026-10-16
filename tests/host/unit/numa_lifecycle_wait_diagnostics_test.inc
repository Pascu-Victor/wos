// Actual userspace clock/wait/cleanup bodies on finite POSIX/sysdep/FD/map ports.
// These cases do not construct a WOS Task, child event or native wait authority.
#include <abi/numa.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string>
#include <vector>
namespace abi = ker::abi::vmem::numa;
constexpr size_t PAGE = 4096;
constexpr size_t CYCLE_BYTES = 3 * PAGE;
/* @ACTUAL_STORAGE@ */
/* @ACTUAL_QUERY_BUFFERS@ */
/* @ACTUAL_CHILD_RECORD@ */
/* @ACTUAL_OWN_RESOURCES@ */
namespace fixture {
unsigned assertions{}, failures{}, cases{};
void check(bool value, const char* why) {
    ++assertions;
    if (!value && failures++ < 24) std::fprintf(stderr, "FAIL: %s\n", why);
}
struct Wait {
    pid_t result;
    int status;
    int error;
};
std::vector<Wait> waits;
size_t wait_index{};
std::vector<std::string> calls;
uint64_t now{}, step{}, deadline{100};
int clock_result{}, clock_error{}, close_error{}, unmap_error{}, query_error{};
int kill_result{}, kill_error{};
int64_t clock_sec_override{-2}, clock_nsec_override{-2};
bool fd_still_open{}, mapping_still_present{};
std::string line;
struct Capture {
    int raw_status{};
    WaitObservation wait{};
    bool wait_owned{}, read_closed{}, reap_needed{}, reaped{}, owned{}, unmapped{}, fds_absent{}, mapping_absent{};
    bool query_valid{};
    int64_t query{};
    OwnResources own{};
    CleanupObservation cleanup{};
} captured;
void reset() {
    ++cases;
    waits.clear();
    wait_index = 0;
    calls.clear();
    line.clear();
    captured = {};
    now = 1;
    step = 0;
    deadline = 100;
    clock_result = clock_error = close_error = unmap_error = query_error = 0;
    kill_result = kill_error = 0;
    clock_sec_override = clock_nsec_override = -2;
    fd_still_open = mapping_still_present = false;
    errno = 0;
}
auto clock_gettime(clockid_t clock, timespec* output) -> int {
    calls.emplace_back("clock");
    check(clock == CLOCK_MONOTONIC, "original clock selector");
    if (clock_result != 0) {
        if (clock_result < 0) errno = clock_error;
        return clock_result;
    }
    output->tv_sec = clock_sec_override == -2 ? static_cast<time_t>(now / 1000000000) : clock_sec_override;
    output->tv_nsec = clock_nsec_override == -2 ? static_cast<long>(now % 1000000000) : clock_nsec_override;
    now += step;
    return 0;
}
auto sysdep_waitpid(pid_t pid, int* status, int flags, void*, pid_t* returned) -> int {
    calls.emplace_back(flags == WNOHANG ? "wait-nohang" : "wait-blocking");
    check(pid == 41, "exact child PID");
    check(wait_index < waits.size(), "finite scripted wait result available");
    if (wait_index >= waits.size()) return ECHILD;
    auto row = waits[wait_index++];
    if (row.result < 0) return row.error;
    if (row.result == pid) *status = row.status;  // Empty kernel WNOHANG does not copy status.
    *returned = row.result;
    return 0;
}
auto posix_waitpid(pid_t, int*, int) -> pid_t;
auto kill(pid_t pid, int signal) -> int {
    calls.emplace_back("kill");
    check(pid == 41 && signal == SIGKILL, "original cleanup kill");
    if (kill_result < 0) errno = kill_error;
    return kill_result;
}
auto close(int fd) -> int {
    calls.emplace_back("close");
    check(fd == 7, "exact read FD close");
    if (close_error) {
        errno = close_error;
        return -1;
    }
    return 0;
}
auto fcntl(int fd, int command, int) -> int {
    calls.emplace_back("confirm-fd");
    check((fd == 7 || fd == 8) && command == F_GETFD, "original closed FD checks");
    if (fd_still_open) return 0;
    errno = EBADF;
    return -1;
}
auto munmap(void* address, size_t bytes) -> int {
    calls.emplace_back("unmap");
    check(address == reinterpret_cast<void*>(0x10000) && bytes == CYCLE_BYTES, "exact mapping cleanup");
    if (unmap_error) {
        errno = unmap_error;
        return -1;
    }
    return 0;
}
auto page_nodes(QueryBuffers& io, const void* mapping, uint32_t pages) -> int64_t {
    calls.emplace_back("query-map");
    check(mapping == reinterpret_cast<void*>(0x10000) && pages == 3, "original mapping absence query");
    if (query_error) return query_error;
    for (unsigned i = 0; i < 3; ++i)
        io.pages[i] = {.node = abi::INVALID_NODE, .status = static_cast<uint16_t>(abi::PageStatus::UNAVAILABLE), .reserved32 = 0};
    if (mapping_still_present) io.pages[1] = {.node = 0, .status = static_cast<uint16_t>(abi::PageStatus::RESIDENT), .reserved32 = 0};
    return 0;
}
auto fprintf(FILE* stream, const char* format, ...) -> int {
    check(stream == stderr && line.empty(), "one existing failure-only print");
    char buffer[2048]{};
    va_list args;
    va_start(args, format);
    int size = std::vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);
    check(size > 0 && static_cast<size_t>(size) < sizeof(buffer), "bounded scalar failure tuple");
    if (size > 0 && static_cast<size_t>(size) < sizeof(buffer)) line.assign(buffer, static_cast<size_t>(size));
    return size;
}
void capture(int status, const WaitObservation& wait, bool wait_owned, bool read_closed, bool reap_needed, bool reaped, bool owned,
             bool unmapped, bool fds_absent, bool query_valid, int64_t query, bool mapping_absent, const OwnResources& own,
             const CleanupObservation& cleanup) {
    captured = {status,   wait,       wait_owned,     read_closed, reap_needed, reaped, owned,
                unmapped, fds_absent, mapping_absent, query_valid, query,       own};
    captured.cleanup = cleanup;
}
}  // namespace fixture
namespace ker::multiproc {
auto yield() -> uint64_t {
    fixture::calls.emplace_back("yield");
    return 0;
}
}  // namespace ker::multiproc
namespace actual_libc {
/* @ACTUAL_LIBC_WAITPID@ */
}
namespace fixture {
auto posix_waitpid(pid_t p, int* s, int flags) -> pid_t { return actual_libc::waitpid(p, s, flags); }
}  // namespace fixture
/* @ACTUAL_CLOCK_WAIT_CLEANUP@ */
/* @ACTUAL_PREDECESSOR@ */
auto run_cleanup(bool ok = true, bool owned = true) -> bool {
    uint32_t cycle = 1;
    pid_t pid = 41;
    uint64_t child_deadline = fixture::deadline;
    const char* phase = "parent_post_copyout";
    int64_t source_query_error = 0;
    ChildRecord record{};
    record.phase = 7;
    record.elapsed_ns = 12345;
    int fds[2]{7, -1}, opened_fds[2]{7, 8};
    OwnResources own{};
    own.fds = 1;
    own.maps = 1;
    own.created_fds = 2;
    own.closed_fds = 1;
    own.created_maps = 1;
    void* mapping = reinterpret_cast<void*>(0x10000);
    QueryBuffers io{};
    do {
        /* @ACTUAL_POSTWAIT@ */
    } while (false);
    return fixture::line.empty();
}
void expect_failure(WaitOutcome outcome, bool status_valid, int error = 0) {
    using namespace fixture;
    check(!run_cleanup(), "actual failure predicate still refuses");
    check(captured.wait.outcome == outcome, "exact wait outcome before cleanup");
    check(captured.wait.status_valid == status_valid, "status validity requires actual PID return");
    check(captured.wait.error == error, "immediate wait errno retained");
    check(line.find("NUMA_LIFECYCLE_FAIL cycle=1 pid=41 phase=wait0 ") == 0, "original failure prefix retained");
    check(line.find("wait_outcome=") != std::string::npos && line.find("clock_valid=") != std::string::npos,
          "bounded tuple printed only on failure");
}
void cleanup_observation_cases() {
    using namespace fixture;
    for (int status : {0, 256, SIGKILL}) {
        reset();
        waits = {{41, status, 0}};
        check(!run_cleanup(false), "earlier refusal retains failure and runs original cleanup");
        check(captured.cleanup.kill_result_valid && captured.cleanup.kill_result == 0 && !captured.cleanup.kill_error_valid &&
                  captured.cleanup.kill_error == 0,
              "successful exact kill tuple");
        check(captured.cleanup.wait_result_valid && captured.cleanup.wait_result == 41 && captured.cleanup.waits == 1 &&
                  captured.cleanup.interrupted_waits == 0 && !captured.cleanup.wait_error_valid && captured.cleanup.wait_error == 0,
              "consumed exact child wait tuple");
        check(captured.cleanup.status_valid && captured.cleanup.status == status && !captured.owned && captured.reaped,
              "consumed normal or signal raw status remains exact");
        check((calls == std::vector<std::string>{"close", "kill", "wait-blocking", "unmap", "confirm-fd", "confirm-fd", "query-map"}),
              "failure cleanup exact original call order with no added clock");
        check(line.find("child_elapsed_raw_ns=12345") != std::string::npos && line.find("cleanup_status_valid=1") != std::string::npos,
              "raw elapsed and cleanup tuple belong only to existing failure line");
    }
    reset();
    kill_result = -1;
    kill_error = ESRCH;
    waits = {{41, 0, 0}};
    check(!run_cleanup(false), "kill failure still consumes original child");
    check(captured.cleanup.kill_result == -1 && captured.cleanup.kill_error_valid && captured.cleanup.kill_error == ESRCH,
          "kill immediate errno survives later EBADF cleanup");
    check(captured.reaped && !captured.owned && errno == EBADF, "signaling refusal preserves successful reap behavior");
    reset();
    waits = {{-1, 0, EINTR}, {41, 9, 0}};
    check(!run_cleanup(false), "original cleanup EINTR retry retains failure predicate");
    check(captured.cleanup.waits == 2 && captured.cleanup.interrupted_waits == 1 && captured.cleanup.status_valid &&
              captured.cleanup.status == 9 && !captured.cleanup.wait_error_valid && captured.cleanup.wait_error == 0,
          "retry counter plus final consumed tuple clears obsolete errno validity");
    check(std::count(calls.begin(), calls.end(), "wait-blocking") == 2 && std::count(calls.begin(), calls.end(), "kill") == 1,
          "no extra cleanup syscall or retry");
    for (int error : {ECHILD, EFAULT}) {
        reset();
        waits = {{-1, 999, error}};
        check(!run_cleanup(false), "wait error preserves cleanup refusal");
        check(captured.cleanup.wait_result == -1 && captured.cleanup.wait_error_valid && captured.cleanup.wait_error == error &&
                  !captured.cleanup.status_valid && captured.cleanup.status == 0 && captured.owned && !captured.reaped,
              "immediate cleanup error does not read invalid status");
    }
    reset();
    waits = {{0, 999, 0}};
    check(!run_cleanup(false), "unexpected nonreaping blocking return still refuses without retry");
    check(captured.cleanup.wait_result_valid && captured.cleanup.wait_result == 0 && !captured.cleanup.status_valid &&
              captured.cleanup.status == 0 && captured.cleanup.waits == 1 && captured.owned,
          "zero-result frontend output is never a consumed status");
    reset();
    check(!run_cleanup(false, false), "earlier refusal with no owned child still prints failure");
    check(!captured.cleanup.kill_result_valid && !captured.cleanup.wait_result_valid && !captured.cleanup.status_valid &&
              captured.cleanup.waits == 0 && captured.cleanup.interrupted_waits == 0,
          "no-owned branch invents no call or observation");
    check(std::count(calls.begin(), calls.end(), "kill") + std::count(calls.begin(), calls.end(), "wait-blocking") == 0,
          "no-owned branch preserves exact no-call behavior");
#if HAVE_WAIT_PREDECESSOR
    for (int which = 0; which < 7; ++which) {
        auto script = which == 0   ? std::vector<Wait>{{41, 0, 0}}
                      : which == 1 ? std::vector<Wait>{{41, SIGKILL, 0}}
                      : which == 2 ? std::vector<Wait>{{-1, 0, EINTR}, {41, 256, 0}}
                      : which == 3 ? std::vector<Wait>{{-1, 999, ECHILD}}
                      : which == 4 ? std::vector<Wait>{{0, 999, 0}}
                                   : std::vector<Wait>{{41, 0, 0}};
        reset();
        waits = script;
        if (which == 5) {
            kill_result = -1;
            kill_error = ESRCH;
        }
        bool old_owned = which != 6;
        bool const old_result = predecessor::terminate_and_reap(41, old_owned);
        auto const old_calls = calls;
        int const old_errno = errno;
        auto const old_consumed = wait_index;
        reset();
        waits = script;
        if (which == 5) {
            kill_result = -1;
            kill_error = ESRCH;
        }
        bool owned = which != 6;
        CleanupObservation observation{};
        bool const result = terminate_and_reap(41, owned, &observation);
        check(result == old_result && owned == old_owned && errno == old_errno && calls == old_calls && wait_index == old_consumed,
              "actual complete predecessor cleanup decisions/ownership/errno/call order remain exact");
    }
#endif
}
auto main() -> int {
    using namespace fixture;
    reset();
    waits = {{41, 0, 0}};
    check(run_cleanup(), "healthy PID0 result remains successful");
    check(line.empty(), "healthy path prints no added observation");
    check(captured.wait.outcome == WaitOutcome::ZERO_EXIT && captured.wait.status_valid && captured.wait.status == 0,
          "consumed zero status valid");
    check(!captured.wait_owned && !captured.reap_needed && captured.reaped && !captured.owned,
          "healthy child already consumed before cleanup");
    check((calls == std::vector<std::string>{"clock", "wait-nohang", "close", "unmap", "confirm-fd", "confirm-fd", "query-map"}),
          "healthy exact old call order/no extra clock");
    for (int status : {256, 9}) {
        reset();
        waits = {{41, status, 0}};
        expect_failure(WaitOutcome::NONZERO_STATUS, true);
        check(captured.wait.status == status && captured.raw_status == status, "actual nonzero/signal raw status");
        check(!captured.reap_needed, "nonzero child consumed before cleanup");
    }
    reset();
    now = 1;
    step = 100;
    waits = {{0, 999, 0}, {41, 0, 0}};
    expect_failure(WaitOutcome::DEADLINE, false);
    check(captured.raw_status == 0, "genuine libc frontend overwrites status on nonreaping zero");
    check(captured.wait.result_valid && captured.wait.result == 0 && captured.wait.polls == 1, "last nonreaping actual return retained");
    check(captured.wait.clock.now == 101 && captured.wait.deadline == 100, "original expired frontier retained");
    check(captured.wait_owned && captured.reap_needed && captured.reaped && !captured.owned,
          "later cleanup reap distinct from measured wait");
    check(captured.own.complete && captured.own.fds == 0 && captured.own.maps == 0 && errno == EBADF,
          "zero debt with stale late EBADF reproduces168 ambiguity");
    check(std::count(calls.begin(), calls.end(), "clock") == 2, "one clock per original wait frontier");
    reset();
    waits = {{-1, 0, EINTR}, {41, 0, 0}};
    check(run_cleanup(), "EINTR still retries");
    check(captured.wait.polls == 2 && !captured.wait.error_valid && captured.wait.error == 0,
          "retry success clears obsolete error validity");
    check(std::count(calls.begin(), calls.end(), "clock") == 2 && std::count(calls.begin(), calls.end(), "yield") == 1,
          "original retry work unchanged");
    for (int error : {ECHILD, EFAULT}) {
        reset();
        waits = {{-1, 0, error}, {41, 0, 0}};
        expect_failure(WaitOutcome::WAIT_ERROR, false, error);
        check(captured.wait.error_valid && captured.wait.result == -1, "actual POSIX error valid before cleanup");
        check(captured.raw_status == -1 && errno == EBADF, "failed frontend leaves status and cleanup overwrites errno only later");
    }
    reset();
    now = 100;
    waits = {{41, 0, 0}};
    expect_failure(WaitOutcome::DEADLINE, false);
    check(!captured.wait.result_valid && captured.wait.polls == 0 && captured.wait.clock.now == 100,
          "expired first frontier performs no measured wait");
    reset();
    clock_result = -1;
    clock_error = EIO;
    waits = {{41, 0, 0}};
    expect_failure(WaitOutcome::CLOCK_ERROR, false);
    check(captured.wait.clock.valid == (CLOCK_RESULT | CLOCK_ERRNO) && captured.wait.clock.result == -1 && captured.wait.clock.error == EIO,
          "actual clock syscall error valid");
    check(!captured.wait.result_valid && captured.wait.polls == 0, "clock failure does not fabricate wait result");
    for (int which = 0; which < 3; ++which) {
        reset();
        if (which == 0) clock_sec_override = -1;
        if (which == 1) clock_nsec_override = 1000000000;
        if (which == 2) clock_sec_override = INT64_MAX;
        waits = {{41, 0, 0}};
        expect_failure(WaitOutcome::CLOCK_ERROR, false);
        check(captured.wait.clock.valid == (CLOCK_RESULT | CLOCK_TIMESPEC) && captured.wait.clock.result == 0 &&
                  captured.wait.clock.error == 0,
              "invalid timespec/overflow not errno failure");
    }
    reset();
    clock_result = 1;
    clock_error = EIO;
    errno = EACCES;
    waits = {{41, 0, 0}};
    expect_failure(WaitOutcome::CLOCK_ERROR, false);
    check(captured.wait.clock.valid == CLOCK_RESULT && captured.wait.clock.error == 0,
          "non-POSIX positive result does not validate stale errno");
    for (int which = 0; which < 4; ++which) {
        reset();
        waits = {{41, 0, 0}};
        if (which == 0) close_error = EIO;
        if (which == 1) unmap_error = EIO;
        if (which == 2) fd_still_open = true;
        if (which == 3) mapping_still_present = true;
        check(!run_cleanup(), "postwait cleanup still refuses independently");
        check(captured.wait.outcome == WaitOutcome::ZERO_EXIT && captured.wait.status_valid,
              "cleanup failure does not replace successful wait tuple");
        check(!captured.own.complete, "actual cleanup ownership becomes incomplete");
        if (which == 0) check(!captured.read_closed && captured.own.fds == 1, "close failure preserves accounting refusal");
        if (which == 1) check(!captured.query_valid && !captured.mapping_absent, "failed unmap causes no query reprobe");
        if (which == 2) check(!captured.fds_absent, "actual FD absence flag");
        if (which == 3) check(!captured.mapping_absent, "actual mapping absence flag");
    }
    reset();
    query_error = -EFAULT;
    waits = {{41, 0, 0}};
    check(!run_cleanup(), "actual mapping query error refuses");
    check(captured.query_valid && captured.query == -EFAULT && !captured.mapping_absent, "raw query error captured without second query");
    reset();
    waits = {{41, 0, 0}};
    check(!run_cleanup(false), "earlier refusal still cleans unconditionally");
    check(captured.wait.outcome == WaitOutcome::NOT_CALLED && !captured.wait.result_valid && captured.wait.clock.valid == 0,
          "earlier phase fabricates no wait/clock observation");
    check(captured.reap_needed && captured.reaped, "earlier refusal consumes cleanup child");
#if HAVE_WAIT_PREDECESSOR
    for (int which = 0; which < 4; ++which) {
        auto script = which == 0   ? std::vector<Wait>{{41, 0, 0}}
                      : which == 1 ? std::vector<Wait>{{-1, 0, EINTR}, {41, 0, 0}}
                      : which == 2 ? std::vector<Wait>{{-1, 0, EFAULT}}
                                   : std::vector<Wait>{{0, 0, 0}};
        reset();
        waits = script;
        if (which == 3) step = 100;
        int old_status = -1;
        bool old_owned = true;
        bool old_ok = predecessor::wait_child(41, deadline, old_status, old_owned);
        auto old_calls = calls;
        int old_errno = errno;
        reset();
        waits = script;
        if (which == 3) step = 100;
        int status = -1;
        bool owned = true;
        WaitObservation observation{};
        bool ok = wait_child(41, deadline, status, owned, observation);
        check(ok == old_ok && status == old_status && owned == old_owned && errno == old_errno && calls == old_calls,
              "actual whole predecessor wait behavior/calls unchanged");
    }
#endif
    cleanup_observation_cases();
    std::printf("%s actual workload wait diagnostics cases=%u assertions=%u failures=%u\n", failures ? "FAIL" : "PASS", cases, assertions,
                failures);
    return failures ? 1 : 0;
}
