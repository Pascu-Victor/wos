#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <pthread.h>
#include <signal.h>
#include <spawn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/auxv.h>
#include <sys/process.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>

namespace {
using Control = ker::abi::process::CredentialControl;
auto control(Control option) -> int64_t { return ker::process::prctl(static_cast<int>(option), 0, 0, 0, 0); }
struct Identity {
    uid_t ur{}, ue{}, us{};
    gid_t gr{}, ge{}, gs{};
    int groups{};
    int64_t nnp{}, grants{}, ceiling{};
    auto operator==(const Identity&) const -> bool = default;
};
auto capture(Identity& value) -> bool {
    if (getresuid(&value.ur, &value.ue, &value.us) || getresgid(&value.gr, &value.ge, &value.gs)) return false;
    value.groups = getgroups(0, nullptr);
    value.nnp = ker::process::prctl(39, 0, 0, 0, 0);
    value.grants = control(Control::GET_SERVICE_PRIVILEGES);
    value.ceiling = control(Control::GET_AUTHORITY_CEILING);
    return value.groups >= 0 && value.nnp >= 0 && value.grants >= 0 && value.ceiling >= 0;
}
auto expected(const Identity& value, const char* profile) -> bool {
    bool const service = strcmp(profile, "plain") != 0;
    bool const inert = strcmp(profile, "inert") == 0;
    return value.ur == 1002 && value.ue == 1002 && value.us == 1002 && value.gr == 2002 && value.ge == 2002 && value.gs == 2002 &&
           value.groups == 0 && value.nnp == service && value.grants == (service ? 3 | ker::abi::process::SERVICE_PRIVILEGES_LOCKED : 0) &&
           value.ceiling == (inert ? 0 : ker::abi::process::CREDENTIAL_AUTHORITY_ALL);
}
auto payload(char** argv) -> int {
    Identity value;
    char host[64]{};
    if (!capture(value) || gethostname(host, sizeof(host))) return 2;
    int const guard = atoi(argv[5]);
    errno = 0;
    bool const closed = fcntl(guard, F_GETFD) == -1 && errno == EBADF;
    bool const secure = strcmp(argv[3], "service") == 0;
    bool const auxv = getauxval(AT_UID) == 1002 && getauxval(AT_EUID) == 1002 && getauxval(AT_GID) == 2002 && getauxval(AT_EGID) == 2002 &&
                      getauxval(AT_SECURE) == static_cast<unsigned long>(secure);
    std::array<char, 512> path{};
    int const length = snprintf(path.data(), path.size(), "%s%s", strcmp(host, "wos-0") == 0 ? "" : "/wki/wos-0", argv[4]);
    if (length < 0 || static_cast<size_t>(length) >= path.size()) return 3;
    int const marker = open(path.data(), O_WRONLY | O_APPEND);
    if (marker < 0) return 4;
    bool const wrote = write(marker, "x", 1) == 1;
    bool const finished = close(marker) == 0;
    bool const ok = expected(value, argv[3]) && strcmp(host, argv[2]) == 0 && closed && auxv && wrote && finished;
    printf("PAYLOAD host=%s pid=%ld identity=%d auxv=%d cloexec=%d marker=%d\n", host, static_cast<long>(getpid()),
           expected(value, argv[3]), auxv, closed, wrote && finished);
    return ok ? 23 : 5;
}
auto no_children() -> bool {
    int status{};
    errno = 0;
    return waitpid(-1, &status, WNOHANG) == -1 && errno == ECHILD;
}
auto attempt(char** argv) -> int {
    Identity before;
    if (!capture(before) || !expected(before, argv[7])) return 3;
    int const guard = open(argv[8], O_RDONLY | O_CLOEXEC);
    if (guard < 0 || fcntl(guard, F_GETFD) != FD_CLOEXEC) return 4;
    bool const strict = strcmp(argv[4], "strict") == 0;
    if (ker::process::setwkitarget(argv[1], strlen(argv[1]), strict ? ker::process::WKI_TARGET_FLAG_STRICT : 0) != 0) return 5;
    char fd[24]{};
    snprintf(fd, sizeof(fd), "%d", guard);
    char flag[] = "--payload";
    char* args[]{argv[2], flag, argv[5], argv[7], argv[8], fd, nullptr};
    bool const spawn = strcmp(argv[3], "spawn") == 0;
    pid_t child = -123;
    int error{};
    if (spawn) {
        error = posix_spawn(&child, argv[2], nullptr, nullptr, args, environ);
        if (error == 0) {
            int status{};
            pid_t waited;
            do {
                waited = waitpid(child, &status, 0);
            } while (waited < 0 && errno == EINTR);
            Identity after;
            bool const unchanged = capture(after) && before == after && fcntl(guard, F_GETFD) == FD_CLOEXEC;
            bool const reaped = waited == child && WIFEXITED(status) && WEXITSTATUS(status) == 23;
            bool const single = no_children();
            printf("SPAWN child=%ld reaped=%d unchanged=%d no_children=%d\n", static_cast<long>(child), reaped, unchanged, single);
            close(guard);
            return reaped && unchanged && single && atoi(argv[6]) == 0 ? 23 : 6;
        }
    } else {
        execv(argv[2], args);
        error = errno;
    }
    Identity after;
    bool const unchanged = capture(after) && before == after;
    bool const preserved = fcntl(guard, F_GETFD) == FD_CLOEXEC;
    bool const single = no_children();
    bool const child_unchanged = child == -123;
    printf("REFUSAL errno=%d unchanged=%d cloexec=%d no_children=%d child_unchanged=%d\n", error, unchanged, preserved, single,
           child_unchanged);
    close(guard);
    return error == atoi(argv[6]) && error != 0 && unchanged && preserved && single && child_unchanged ? 0 : 7;
}
}  // namespace

int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    alarm(45);
    if (argc == 6 && strcmp(argv[1], "--payload") == 0) return payload(argv);
    if (argc != 9 || geteuid() != 0) return 2;
    pid_t const child = fork();
    if (child == 0) {
        bool const service = strcmp(argv[7], "plain") != 0;
        if (service &&
            (ker::process::prctl(38, 1, 0, 0, 0) || ker::process::prctl(static_cast<int>(Control::SET_SERVICE_PRIVILEGES), 3, 0, 0, 0)))
            _exit(3);
        if (setgroups(0, nullptr) || setgid(2002) || setuid(1002)) _exit(4);
        if (strcmp(argv[7], "inert") == 0 && ker::process::prctl(static_cast<int>(Control::DROP_AUTHORITY_CEILING), 0, 0, 0, 0)) _exit(5);
        if (strcmp(argv[3], "worker") == 0) {
            pthread_t worker;
            if (pthread_create(
                    &worker, nullptr,
                    [](void* p) -> void* { return reinterpret_cast<void*>(static_cast<intptr_t>(attempt(static_cast<char**>(p)))); }, argv))
                _exit(8);
            void* result{};
            _exit(pthread_join(worker, &result) ? 9 : static_cast<int>(reinterpret_cast<intptr_t>(result)));
        }
        _exit(attempt(argv));
    }
    if (child < 0) return 10;
    int status{};
    pid_t waited;
    do {
        waited = waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    bool const reaped = waited == child && WIFEXITED(status);
    bool const single = no_children();
    int const code = reaped ? WEXITSTATUS(status) : -1;
    printf("COMPLETE child=%ld reaped=%d exit=%d no_children=%d\n", static_cast<long>(child), reaped, code, single);
    return reaped && single && code == (atoi(argv[6]) == 0 ? 23 : 0) ? 0 : 11;
}
