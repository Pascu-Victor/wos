// Run the unprivileged soak witness, then request ordinary init shutdown.
#include <grp.h>
#include <signal.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>

namespace {
using Control = ker::abi::process::CredentialControl;
constexpr uint64_t SIGNAL = uint64_t{1} << static_cast<unsigned>(ker::abi::process::CredentialAuthority::SIGNAL);
constexpr uint64_t SET_IDS = (uint64_t{1} << static_cast<unsigned>(ker::abi::process::CredentialAuthority::SET_UID)) |
                             (uint64_t{1} << static_cast<unsigned>(ker::abi::process::CredentialAuthority::SET_GID));
constexpr uint64_t INITIAL_CEILING = SET_IDS | SIGNAL;
constexpr uint64_t SECOND = 1'000'000'000;
constexpr uint64_t WITNESS_LIMIT = 7'480 * SECOND;  // Unchanged witness limit7470s plus10s.
constexpr uint64_t CLEANUP_LIMIT = 5 * SECOND;
constexpr char WITNESS[] = "/usr/libexec/goal39-twohour-runtime";
static_assert(INITIAL_CEILING == 19 && SIGNAL == 16);

auto control(Control op, uint64_t value = 0) -> int64_t { return ker::process::prctl(static_cast<int>(op), value, 0, 0, 0); }

auto identity(uint32_t uid, uint32_t gid, uint64_t ceiling) -> bool {
    uid_t real_uid{}, effective_uid{}, saved_uid{};
    gid_t real_gid{}, effective_gid{}, saved_gid{};
    return ::getresuid(&real_uid, &effective_uid, &saved_uid) == 0 && ::getresgid(&real_gid, &effective_gid, &saved_gid) == 0 &&
           real_uid == uid && effective_uid == uid && saved_uid == uid && real_gid == gid && effective_gid == gid && saved_gid == gid &&
           ::getgroups(0, nullptr) == 0 && control(Control::GET_AUTHORITY_CEILING) == static_cast<int64_t>(ceiling) &&
           control(Control::GET_NO_NEW_PRIVILEGES) == 1 &&
           control(Control::GET_SERVICE_PRIVILEGES) == static_cast<int64_t>(ker::abi::process::SERVICE_PRIVILEGES_LOCKED) &&
           ::umask(0077) == 0077;
}

auto now_ns() -> uint64_t {
    timespec value{};
    if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0 || value.tv_sec < 0 || value.tv_nsec < 0 || value.tv_nsec >= 1'000'000'000)
        return UINT64_MAX;
    auto const seconds = static_cast<uint64_t>(value.tv_sec);
    auto const fraction = static_cast<uint64_t>(value.tv_nsec);
    return seconds > (UINT64_MAX - fraction) / SECOND ? UINT64_MAX : seconds * SECOND + fraction;
}

auto pause_poll() -> bool {
    timespec const delay{.tv_sec = 0, .tv_nsec = 100'000'000};
    return ::nanosleep(&delay, nullptr) == 0 || errno == EINTR;
}

struct Child {
    int64_t pid{};
    bool owned{};

    // A consuming wait or ECHILD ends numeric-PID authority. Never kill later
    // after either result; that PID could be reused by an unrelated process.
    auto poll(int32_t& status) -> int64_t {
        auto const result = ker::process::waitpid(pid, &status, WNOHANG, nullptr);
        if (result == pid || result == -ECHILD) owned = false;
        return result;
    }

    void cleanup() {
        if (!owned) return;
        (void)ker::process::kill(pid, SIGKILL);
        auto const started = now_ns();
        if (started == UINT64_MAX || started > UINT64_MAX - CLEANUP_LIMIT) {
            int32_t status{};
            (void)poll(status);
            return;  // Fail closed; init retains ordinary orphan/group cleanup.
        }
        auto const deadline = started + CLEANUP_LIMIT;
        while (owned) {
            int32_t status{};
            auto const result = poll(status);
            if (!owned || (result != 0 && result != -EINTR)) return;
            auto const now = now_ns();
            if (now == UINT64_MAX || now >= deadline || !pause_poll()) return;
        }
    }
};

[[noreturn]] void run_witness(int64_t wrapper) {
    // Keep SET_GID/SET_UID until each permanent ID drop has succeeded.
    if (ker::process::getppid() != static_cast<uint64_t>(wrapper) || control(Control::SET_NO_NEW_PRIVILEGES, 1) != 0 ||
        control(Control::SET_SERVICE_PRIVILEGES, 0) != 0 || ::setgroups(0, nullptr) != 0 || ::setgid(100) != 0 || ::setuid(100) != 0)
        ::_exit(71);
    (void)::umask(0077);
    if (control(Control::DROP_AUTHORITY_CEILING, 0) != 0 || !identity(100, 100, 0) ||
        ker::process::getppid() != static_cast<uint64_t>(wrapper))
        ::_exit(71);
    const char* const arguments[]{WITNESS, "--init-exit-witness", nullptr};
    const char* const environment[]{"LD_LIBRARY_PATH=/usr/lib", nullptr};
    (void)ker::process::execve(WITNESS, arguments, environment);
    ::_exit(72);  // Every exec return is failure, including a malformed success.
}
}  // namespace

auto main(int argc, char**) -> int {
    // This proposal is only for the actual original init's root service in its
    // inherited Host namespaces. getppid1 alone is not namespace attestation.
    if (argc != 1 || ker::process::getppid() != 1 || !identity(0, 0, INITIAL_CEILING)) return 70;
    auto const wrapper = ker::process::getpid();
    auto const started = now_ns();
    if (wrapper <= 1 || wrapper > INT64_MAX || started == UINT64_MAX || started > UINT64_MAX - WITNESS_LIMIT) return 70;
    auto const pid = ker::process::fork();
    if (pid < 0) return 73;
    if (pid == 0) run_witness(static_cast<int64_t>(wrapper));
    if (pid <= 1 || pid == static_cast<int64_t>(wrapper)) return 73;
    Child child{pid, true};
    auto fail = [&child](int code) {
        child.cleanup();
        return code;
    };
    if (control(Control::DROP_AUTHORITY_CEILING, SIGNAL) != 0 || !identity(0, 0, SIGNAL)) return fail(74);
    auto const deadline = started + WITNESS_LIMIT;
    for (;;) {
        int32_t status = -1;
        auto const result = child.poll(status);
        if (result == pid) {
            if (status != 0) return 75;
            break;
        }
        if (result != 0 && result != -EINTR) return fail(76);
        auto const now = now_ns();
        if (now == UINT64_MAX || now >= deadline || !pause_poll()) return fail(77);
    }
    // Exact successful child consumption is the only route to this operation.
    if (ker::process::getppid() != 1 || !identity(0, 0, SIGNAL)) return 78;
    if (ker::process::kill(1, SIGUSR2) != 0) return 79;
    return 0;  // Signal admission is not shutdown, filesystem or report proof.
}
