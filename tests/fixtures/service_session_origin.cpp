#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {
auto reap(pid_t child) -> int {
    int status = 0;
    pid_t result;
    do {
        result = waitpid(child, &status, 0);
    } while (result < 0 && errno == EINTR);
    return result == child && WIFEXITED(status) ? WEXITSTATUS(status) : 3;
}

auto member_after_exec(pid_t parent_group, pid_t inherited_session) -> int {
    bool const INHERITED = getsid(0) == inherited_session && getpgrp() == getpid();
    errno = 0;
    bool const LEADER_DENIED = setsid() == -1 && errno == EPERM;
    bool const JOINED =
        INHERITED && LEADER_DENIED && setpgid(0, parent_group) == 0 && getpgrp() == parent_group && getsid(0) == inherited_session;
    bool const NEW_SESSION = JOINED && setsid() == getpid();
    printf("%s group origin survives fork/exec, join and new session\n", NEW_SESSION ? "PASS" : "FAIL");
    return NEW_SESSION ? 0 : 1;
}

auto group_transition(const char* self) -> int {
    pid_t const SID = getsid(0);
    bool const CREATED = setpgid(0, 0) == 0 && getpgrp() == getpid() && getsid(0) == SID;
    errno = 0;
    bool const LEADER_DENIED = CREATED && setsid() == -1 && errno == EPERM;
    int member_status = 3;
    if (LEADER_DENIED) {
        std::array<char, 24> group{}, session{};
        snprintf(group.data(), group.size(), "%ld", static_cast<long>(getpgrp()));
        snprintf(session.data(), session.size(), "%ld", static_cast<long>(SID));
        pid_t const MEMBER = fork();
        if (MEMBER == 0) {
            if (setpgid(0, 0)) _exit(3);
            execl(self, self, "--member", group.data(), session.data(), nullptr);
            _exit(3);
        }
        if (MEMBER > 0) member_status = reap(MEMBER);
    }
    bool const PASS = CREATED && LEADER_DENIED && member_status == 0;
    printf("%s local group within imported session created=%d leader_denied=%d member_reaped_status=%d\n", PASS ? "PASS" : "FAIL", CREATED,
           LEADER_DENIED, member_status);
    return PASS ? 0 : 1;
}

auto receiver(pid_t source_session, bool group_case, const char* self) -> int {
    // Each padding child exits and is reaped before the next is created. The
    // aligned child inherits a foreign session/group; equal numeric IDs do
    // not make it the process that created either of those objects.
    for (unsigned attempt = 0; attempt < 512; ++attempt) {
        pid_t child = fork();
        if (child < 0) return 3;
        if (child == 0) {
            if (getpid() != source_session) _exit(0);
            pid_t const BEFORE_SID = getsid(0);
            pid_t const BEFORE_GROUP = getpgrp();
            bool const ALIGNED = BEFORE_SID == getpid() && BEFORE_GROUP == getpid();
            if (group_case) {
                int const RESULT = ALIGNED ? group_transition(self) : 3;
                fflush(stdout);
                _exit(RESULT);
            }
            errno = 0;
            pid_t const RESULT = setsid();
            int const ERROR = errno;
            bool const PASS = ALIGNED && RESULT == getpid() && getsid(0) == getpid() && getpgrp() == getpid();
            printf("%s foreign-origin setsid pid=%ld inherited_sid=%ld inherited_pgid=%ld result=%ld errno=%d aligned=%d\n",
                   PASS ? "PASS" : "FAIL", static_cast<long>(getpid()), static_cast<long>(BEFORE_SID), static_cast<long>(BEFORE_GROUP),
                   static_cast<long>(RESULT), ERROR, ALIGNED);
            fflush(stdout);
            _exit(PASS ? 0 : 1);
        }
        int const STATUS = reap(child);
        if (child == source_session) {
            printf("receiver aligned child reaped status=%d padding=%u\n", STATUS, attempt);
            return STATUS;
        }
        if (STATUS != 0 || child > source_session) {
            printf("SETUP could not align receiver pid=%ld source_sid=%ld status=%d\n", static_cast<long>(child),
                   static_cast<long>(source_session), STATUS);
            return 3;
        }
    }
    return 3;
}

auto driver(const char* peer, const char* remote_helper, const char* mode) -> int {
    if (setsid() != getpid()) return 3;
    errno = 0;
    bool const LOCAL_LEADER_DENIED = setsid() == -1 && errno == EPERM;
    printf("%s local session/group leader setsid denied\n", LOCAL_LEADER_DENIED ? "PASS" : "FAIL");
    fflush(stdout);
    if (!LOCAL_LEADER_DENIED) return 1;
    std::array<char, 24> session{};
    snprintf(session.data(), session.size(), "%ld", static_cast<long>(getsid(0)));
    pid_t child = fork();
    if (child < 0) return 3;
    if (child == 0) {
        execl("/usr/bin/on", "on", peer, remote_helper, "--receiver", session.data(), mode, nullptr);
        _exit(3);
    }
    int const STATUS = reap(child);
    printf("driver receiver reaped status=%d\n", STATUS);
    return STATUS;
}
}  // namespace

int main(int argc, char** argv) {
    alarm(45);
    if (argc == 4 && std::strcmp(argv[1], "--receiver") == 0)
        return receiver(static_cast<pid_t>(std::strtol(argv[2], nullptr, 10)), std::strcmp(argv[3], "group") == 0, argv[0]);
    if (argc == 4 && std::strcmp(argv[1], "--member") == 0)
        return member_after_exec(static_cast<pid_t>(std::strtol(argv[2], nullptr, 10)),
                                 static_cast<pid_t>(std::strtol(argv[3], nullptr, 10)));
    if (argc == 3) {
        // Start from a fresh local child, independent of SSH's own group setup.
        int result = 0;
        for (const char* mode : {"session", "group"}) {
            pid_t const CHILD = fork();
            if (CHILD < 0) return 3;
            if (CHILD == 0) return driver(argv[1], argv[2], mode);
            result |= reap(CHILD);
        }
        return result;
    }
    return 3;
}
