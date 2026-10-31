// Actual owner definitions are linked separately. These are Host-only syscall
// adaptation, short-time fixtures and assertions, not a second ownership model.
#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string>

#include "uma_reference_arm.hpp"
#include "uma_reference_owner.hpp"

namespace {
unsigned checks{}, failed{}, calls_after_consume{}, peek_calls{}, consume_calls{}, signal_calls{};
bool consumed{}, late_cancel{};
const char* fixture_root{};
const char* fixture_mode{};
bool forbid_allocations{};
bool inspection_faults_active{};
unsigned stat_faults{}, read_faults{}, cleanup_faults{}, real_cleanup_failures{};
int injected_directory_fd{-1};
DIR* close_fault_stream{};
void check(bool value, const char* message) {
    ++checks;
    if (!value) {
        ++failed;
        std::fprintf(stderr, "FAIL %s\n", message);
    }
}
auto clock_ns() -> uint64_t {
    timespec t{};
    if (::clock_gettime(CLOCK_MONOTONIC, &t) != 0) std::abort();
    return uint64_t(t.tv_sec) * 1'000'000'000 + uint64_t(t.tv_nsec);
}
[[noreturn]] void hold() {
    for (;;) ::pause();
}
void file(const std::string& path, const std::string& bytes) {
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd < 0 || ::write(fd, bytes.data(), bytes.size()) != static_cast<ssize_t>(bytes.size()) || ::close(fd) != 0) ::_exit(93);
}
auto fixture_child(const char* mode, const char* root) -> int {
    // This executes only after the real owner setup/acknowledgment/exec path.
    struct stat a{}, b{}, c{};
    char byte;
    if (::getpid() != ::getsid(0) || ::getpid() != ::getpgrp() || ::isatty(0) || ::isatty(1) || ::isatty(2) || ::read(0, &byte, 1) != 0 ||
        ::fstat(0, &a) || !S_ISCHR(a.st_mode) || ::fstat(1, &b) || ::fstat(2, &c) || !S_ISREG(b.st_mode) || b.st_ino != c.st_ino ||
        b.st_dev != c.st_dev)
        return 94;
    int tty = ::open("/dev/tty", O_RDONLY | O_NOCTTY);
    if (tty >= 0 || errno != ENXIO) return 95;
    if (std::strcmp(mode, "success") == 0 || std::strcmp(mode, "success-empty") == 0 || std::strcmp(mode, "late") == 0) return 0;
    if (std::strcmp(mode, "raw") == 0) return 7;
    // Actual parent acknowledgment observation, exported only by the Host seam,
    // makes the cancellation-after-admission vector independent of scheduling.
    uint64_t admitted_by = clock_ns() + 1'000'000'000;
    struct stat admitted{};
    while (::stat((std::string(root) + "/host-admitted").c_str(), &admitted) != 0) {
        if (clock_ns() >= admitted_by) return 102;
        timespec t{0, 1'000'000};
        (void)::nanosleep(&t, nullptr);
    }
    if (std::strcmp(mode, "timeout") == 0)
        file(std::string(root) + "/warmup/work/testprog_smoke.timeout", "timeout\n");
    else if (std::strcmp(mode, "output") == 0) {
        if (::ftruncate(1, 16 * 1024 * 1024 + 1) != 0) return 96;
    } else if (std::strcmp(mode, "output-total") == 0) {
        // Sparse files exercise the unchanged aggregate logical-byte bound
        // without allocating their 80MiB of apparent output on the host disk.
        for (unsigned i = 0; i < 5; ++i) {
            std::string path = std::string(root) + "/warmup/aggregate-" + std::to_string(i);
            int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
            if (fd < 0 || ::ftruncate(fd, 16 * 1024 * 1024) != 0 || ::close(fd) != 0) return 103;
        }
    } else if (std::strcmp(mode, "summary-mode") == 0) {
        // A real file, created with its wrong mode at open, only after the
        // parent's actual admission. No chmod window or synthetic stat data.
        file(std::string(root) + "/warmup/summary.tsv", "");
    } else if (std::strcmp(mode, "cancel") == 0) {
        if (::kill(::getppid(), SIGTERM) != 0) return 97;
    } else if (std::strcmp(mode, "escape") == 0) {
        pid_t escape = ::fork();
        if (escape < 0) return 98;
        if (escape == 0) {
            if (::setsid() != ::getpid()) ::_exit(99);
            file(std::string(root) + "/escape.pid", std::to_string(::getpid()) + "\n");
            hold();
        }
        // Wait only for the owned descendant's completed identity publication;
        // the outer Host subreaper retains and cleans the real escaped process.
        uint64_t end = clock_ns() + 1'000'000'000;
        struct stat st{};
        while (::stat((std::string(root) + "/escape.pid").c_str(), &st) != 0) {
            if (clock_ns() >= end) return 100;
            timespec t{0, 1'000'000};
            (void)::nanosleep(&t, nullptr);
        }
        if (::kill(::getppid(), SIGTERM) != 0) return 101;
    }
    hold();
}
}  // namespace

// No C++ exception may abandon a still-owned child. The actual owner's
// post-admission polling uses fixed buffers; setup strings precede fork.
void* operator new(std::size_t bytes) {
    if (forbid_allocations) throw std::bad_alloc();
    void* result = std::malloc(bytes ? bytes : 1);
    if (!result) throw std::bad_alloc();
    return result;
}
void operator delete(void* value) noexcept { std::free(value); }
void operator delete(void* value, std::size_t) noexcept { std::free(value); }

extern "C" auto __real_fstatat(int, const char*, struct stat*, int) -> int;
extern "C" auto __real_readdir(DIR*) -> dirent*;
extern "C" auto __real_closedir(DIR*) -> int;

// These link-time seams affect only the Host owner TU, after its real child
// admission. All unselected calls retain their real libc/kernel behavior.
extern "C" auto __wrap_fstatat(int dir, const char* name, struct stat* metadata, int flags) -> int {
    if (inspection_faults_active && ((!std::strcmp(fixture_mode, "stat-eio-cleanup") && !std::strcmp(name, "summary.tsv")) ||
                                     (!std::strcmp(fixture_mode, "phase-log-enoent") && !std::strcmp(name, "phase.log")))) {
        ++stat_faults;
        injected_directory_fd = dir;
        errno = !std::strcmp(fixture_mode, "stat-eio-cleanup") ? EIO : ENOENT;
        return -1;
    }
    return __real_fstatat(dir, name, metadata, flags);
}
extern "C" auto __wrap_readdir(DIR* stream) -> dirent* {
    if (inspection_faults_active && !std::strcmp(fixture_mode, "close-eio") && !close_fault_stream) {
        // Select the outer retained walk, not an earlier nested log-directory
        // cleanup. This preserves a deterministic first failure at depth zero.
        close_fault_stream = stream;
        injected_directory_fd = ::dirfd(stream);
    }
    if (inspection_faults_active && !std::strcmp(fixture_mode, "readdir-eio-cleanup")) {
        ++read_faults;
        injected_directory_fd = ::dirfd(stream);
        errno = EIO;
        return nullptr;
    }
    return __real_readdir(stream);
}
extern "C" auto __wrap_closedir(DIR* stream) -> int {
    bool const close_only =
        inspection_faults_active && !std::strcmp(fixture_mode, "close-eio") && cleanup_faults == 0 && stream == close_fault_stream;
    int const result = __real_closedir(stream);
    if (close_only || (inspection_faults_active && (stat_faults || read_faults) &&
                       (!std::strcmp(fixture_mode, "stat-eio-cleanup") || !std::strcmp(fixture_mode, "readdir-eio-cleanup")))) {
        ++cleanup_faults;
        if (result != 0) ++real_cleanup_failures;
        // Close the actual stream first, then model a failed cleanup that
        // overwrites errno and attempts to replace the first failure detail.
        errno = close_only ? EIO : ESTALE;
        return -1;
    }
    return result;
}

auto uma_owner_host_waitpid(pid_t pid, int* raw, int options) -> pid_t {
    if (consumed) ++calls_after_consume;
    if (options & WNOWAIT) {
        ++peek_calls;
        siginfo_t info{};
        int flags = WEXITED | WNOWAIT;
        if (options & WNOHANG) flags |= WNOHANG;
        if (::waitid(P_PID, static_cast<id_t>(pid), &info, flags) != 0) return -1;
        if (info.si_pid == 0) return 0;
        if (info.si_pid != pid || (info.si_code != CLD_EXITED && info.si_code != CLD_KILLED && info.si_code != CLD_DUMPED)) {
            errno = ECHILD;
            return -1;
        }
        *raw = info.si_code == CLD_EXITED ? info.si_status << 8 : info.si_status | (info.si_code == CLD_DUMPED ? 0x80 : 0);
        return info.si_pid;
    }
    ++consume_calls;
    pid_t result = ::waitpid(pid, raw, options);
    if (result == pid) consumed = true;
    return result;
}
auto uma_owner_host_kill(pid_t target, int sig) -> int {
    ++signal_calls;
    if (consumed) ++calls_after_consume;
    // The real kernel is still retaining the exact direct child here.
    siginfo_t info{};
    int rc = ::waitid(P_PID, static_cast<id_t>(target < 0 ? -target : target), &info, WEXITED | WNOHANG | WNOWAIT);
    check(rc == 0, "actual nonconsuming child authority immediately before signal");
    return ::kill(target, sig);
}
void uma_owner_host_after_consume() {
    if (late_cancel) check(::kill(::getpid(), SIGTERM) == 0, "actual late TERM delivered after final consuming wait");
}
void uma_owner_host_admitted() {
    file(std::string(fixture_root) + "/host-admitted", "ADMITTED\n");
    forbid_allocations = true;
    inspection_faults_active = true;
}

auto main(int argc, char** argv) -> int {
    if (argc == 4 && std::strcmp(argv[1], "--child") == 0) return fixture_child(argv[2], argv[3]);
    if (argc != 3) return 2;
    const char* mode = argv[1];
    const char* root = argv[2];
    fixture_root = root;
    fixture_mode = mode;
    char* args[]{argv[0], const_cast<char*>("--child"), argv[1], argv[2], nullptr};
    late_cancel = std::strcmp(mode, "late") == 0;
    uma_reference::PhaseRequest request{};
    uma_reference::arm::Cancellation cancellation;
    check(cancellation.install(), "one whole-arm scalar cancellation installed");
    request.cancellation = &cancellation.value;
    request.phase = "warmup";
    request.nonce = "0123456789abcdef0123456789abcdef";
    request.work_stop_ns = clock_ns() + (std::strcmp(mode, "deadline") == 0 ? 150'000'000 : 3'000'000'000);
    request.host_root = root;
    request.host_argv = args;
    request.host_grace_ns = 50'000'000;
    auto outcome = uma_reference::own_suite_phase(request);
    if (late_cancel) {
        unsigned before = consume_calls;
        request.phase = "measure";
        auto next = uma_reference::own_suite_phase(request);
        check(next.failure == uma_reference::OwnerFailure::CANCELLED && next.child < 0 && consume_calls == before,
              "persistent late cancellation prevents actual second phase/fork without reset");
    }
    inspection_faults_active = false;
    forbid_allocations = false;
    using F = uma_reference::OwnerFailure;
    F wanted = F::NONE;
    if (!std::strcmp(mode, "raw"))
        wanted = F::RAW_STATUS;
    else if (!std::strcmp(mode, "cancel") || !std::strcmp(mode, "late") || !std::strcmp(mode, "escape"))
        wanted = F::CANCELLED;
    else if (!std::strcmp(mode, "deadline"))
        wanted = F::DEADLINE;
    else if (!std::strcmp(mode, "timeout"))
        wanted = F::TIMEOUT_MARKER;
    else if (!std::strcmp(mode, "output") || !std::strcmp(mode, "output-total") || !std::strcmp(mode, "stat-eio-cleanup") ||
             !std::strcmp(mode, "phase-log-enoent") || !std::strcmp(mode, "readdir-eio-cleanup") || !std::strcmp(mode, "close-eio") ||
             !std::strcmp(mode, "summary-mode"))
        wanted = F::OUTPUT;
    else if (!std::strcmp(mode, "setup"))
        wanted = F::SETUP;
    check(outcome.failure == wanted, "actual owner exact failure reason");
    using I = uma_reference::InspectionCheck;
    auto const& detail = outcome.inspection;
    bool const bounded_entry = std::memchr(detail.entry.data(), '\0', detail.entry.size()) != nullptr;
    check(bounded_entry && !detail.entry_truncated, "inspection entry is bounded and complete for fixture names");
    auto entry_is = [&](const char* name) { return bounded_entry && !std::strcmp(detail.entry.data(), name); };
    if (!std::strcmp(mode, "stat-eio-cleanup") || !std::strcmp(mode, "readdir-eio-cleanup")) {
        bool const stat_error = !std::strcmp(mode, "stat-eio-cleanup");
        check(detail.check == (stat_error ? I::STAT_ENTRY : I::READ_DIRECTORY), "first syscall predicate survives cleanup failure");
        check(entry_is(stat_error ? "summary.tsv" : "."), "first syscall entry survives cleanup failure");
        check(detail.syscall_error_valid && detail.system_error == EIO, "first syscall immediate EIO survives cleanup ESTALE");
        check(!detail.metadata_valid && detail.syscall_result == (stat_error ? -1 : 0), "failed syscall has no stat metadata");
        check(detail.directory_fd >= 0 && detail.directory_fd == injected_directory_fd && detail.depth == 0,
              "first syscall retains actual walk descriptor and depth");
        check((stat_error ? stat_faults == 1 && read_faults == 0 : read_faults == 1 && stat_faults == 0),
              "exact selected post-admission syscall fault reached");
        check(cleanup_faults == 1 && real_cleanup_failures == 0, "real directory stream closed after first failure");
        check(outcome.system_error == ESTALE, "legacy post-cleanup system_error remains ESTALE");
    } else if (!std::strcmp(mode, "close-eio")) {
        check(detail.check == I::CLOSE_DIRECTORY && entry_is("."), "cleanup can be the first rejected inspection predicate");
        check(detail.syscall_error_valid && detail.system_error == EIO && detail.syscall_result == -1 && !detail.metadata_valid,
              "first cleanup failure retains its own immediate syscall error");
        check(detail.directory_fd >= 0 && detail.directory_fd == injected_directory_fd && detail.depth == 0,
              "first cleanup failure retains actual stream descriptor and depth");
        check(cleanup_faults == 1 && real_cleanup_failures == 0 && stat_faults == 0 && read_faults == 0,
              "close-only fault follows exactly one successful actual stream close");
        check(outcome.system_error == EIO, "legacy close-only system_error remains EIO");
    } else if (!std::strcmp(mode, "phase-log-enoent")) {
        check(detail.check == I::STAT_ENTRY && entry_is("phase.log"), "retained phase log ENOENT is not optional absence");
        check(detail.syscall_error_valid && detail.system_error == ENOENT && detail.syscall_result == -1 && !detail.metadata_valid,
              "retained phase log reports immediate missing-entry syscall error");
        check(stat_faults == 1 && detail.directory_fd == injected_directory_fd && detail.depth == 0,
              "retained log failure is the actual post-admission walk");
    } else if (!std::strcmp(mode, "summary-mode") || !std::strcmp(mode, "output") || !std::strcmp(mode, "output-total") ||
               !std::strcmp(mode, "timeout")) {
        I const expected = !std::strcmp(mode, "summary-mode")   ? I::MODE
                           : !std::strcmp(mode, "output")       ? I::FILE_SIZE
                           : !std::strcmp(mode, "output-total") ? I::OUTPUT_BYTES
                                                                : I::TIMEOUT_MARKER;
        check(detail.check == expected, "exact synthetic metadata or cap predicate");
        check(!detail.syscall_error_valid && detail.system_error == 0 && detail.syscall_result == 0,
              "synthetic predicate never attributes stale errno to a successful syscall");
        check(detail.metadata_valid && detail.directory_fd >= 0 && detail.depth == 0, "actual metadata and directory context retained");
        if (detail.metadata_valid) {
            check(S_ISREG(static_cast<mode_t>(detail.mode)) && detail.uid == ::getuid() && detail.gid == ::getgid() && detail.links == 1 &&
                      detail.inode != 0,
                  "real file type ownership inode and unique link metadata retained");
            if (expected == I::MODE)
                check(entry_is("summary.tsv") && detail.size == 0 && (detail.mode & 07777) == 0600 && detail.observed == 0600 &&
                          detail.limit == 0644,
                      "real empty summary mode rejected without chmod window");
            else if (expected == I::FILE_SIZE)
                check(entry_is("phase.log") && detail.size == 16 * 1024 * 1024 + 1 && detail.observed == 16 * 1024 * 1024 + 1 &&
                          detail.limit == 16 * 1024 * 1024,
                      "sparse per-file overflow retains exact original cap");
            else if (expected == I::OUTPUT_BYTES)
                check(bounded_entry && !std::strncmp(detail.entry.data(), "aggregate-", 10) && detail.size == 16 * 1024 * 1024 &&
                          detail.observed == 80 * 1024 * 1024 && detail.limit == 64 * 1024 * 1024,
                      "sparse aggregate overflow retains exact original cap");
            else
                check(entry_is("testprog_smoke.timeout"), "timeout marker retains actual case entry");
        }
    } else {
        check(detail.check == I::NONE && !detail.syscall_error_valid && !detail.metadata_valid && detail.directory_fd == -1 &&
                  detail.entry[0] == '\0',
              "success optional absence and non-inspection failures have no inspection detail");
    }
    check(!outcome.descendant_closure_proven, "no universal descendant closure inferred");
    check(calls_after_consume == 0, "no wait or signal after consuming membership");
    if (wanted == F::SETUP) {
        check(outcome.child < 0 && consume_calls == 0 && signal_calls == 0, "setup refusal precedes child/resources");
    } else {
        check(outcome.admitted && outcome.child > 1, "positive actual session/group/FD acknowledgment");
        check(outcome.consumed && outcome.consuming_completions == 1 && consume_calls == 1, "exact one consuming wait");
        check(outcome.nonconsuming_completions > 0 && peek_calls > 0, "actual WNOWAIT observation precedes consumption");
        check(outcome.observed_raw == outcome.consumed_raw, "nonconsuming and consuming raw status identical");
        check(outcome.raw_phase_zero == (wanted == F::NONE), "phase-only raw zero never promotes failure");
        if (wanted == F::NONE || !std::strcmp(mode, "late"))
            check(signal_calls == 0, "no abort signals on normal or late-completed child");
        else
            check(outcome.term_calls == 1 && outcome.kill_calls == 1 && signal_calls == 2, "TERM and KILL before consumption");
    }
    if (!std::strcmp(mode, "escape")) {
        FILE* f = std::fopen((std::string(root) + "/escape.pid").c_str(), "r");
        int pid{};
        check(f && std::fscanf(f, "%d", &pid) == 1 && pid > 1, "actual escaped descendant identity saved");
        if (f) std::fclose(f);
        check(pid > 1 && ::kill(pid, 0) == 0 && ::getpgid(pid) == pid, "actual separate group still alive: closure UNKNOWN/FAIL");
    }
    std::printf(
        "OWNER_CASE mode=%s checks=%u failures=%u failure=%u raw=%d consumed=%d peek=%u consume=%u signals=%u after=%u closure=UNKNOWN\n",
        mode, checks, failed, unsigned(outcome.failure), outcome.observed_raw, outcome.consumed_raw, peek_calls, consume_calls,
        signal_calls, calls_after_consume);
    std::printf(
        "INSPECTION_CASE mode=%s check=%u errno_valid=%u error=%d metadata=%u legacy_error=%d stat_faults=%u read_faults=%u "
        "cleanup_faults=%u entry=%.128s\n",
        mode, unsigned(detail.check), unsigned(detail.syscall_error_valid), detail.system_error, unsigned(detail.metadata_valid),
        outcome.system_error, stat_faults, read_faults, cleanup_faults, detail.entry.data());
    return failed ? 1 : 0;
}
