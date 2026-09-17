#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <errno.h>
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

namespace {
auto report(unsigned eu, unsigned eg, const char* expected_host, const char* profile) -> int {
    uid_t ur{}, ue{}, us{};
    gid_t gr{}, ge{}, gs{};
    char host[64]{};
    if (getresuid(&ur, &ue, &us) || getresgid(&gr, &ge, &gs) || gethostname(host, sizeof(host))) return 2;
    bool const identity = ur == 1002 && ue == eu && us == eu && gr == 2002 && ge == eg && gs == eg && strcmp(host, expected_host) == 0;
    errno = 0;
    int const group_result = setegid(2000), group_error = errno;
    errno = 0;
    int const user_result = seteuid(1000), user_error = errno;
    bool const discarded = group_result == -1 && group_error == EPERM && user_result == -1 && user_error == EPERM;
    auto const grants = ker::process::prctl(static_cast<int>(ker::abi::process::CredentialControl::GET_SERVICE_PRIVILEGES), 0, 0, 0, 0);
    auto const ceiling = ker::process::prctl(static_cast<int>(ker::abi::process::CredentialControl::GET_AUTHORITY_CEILING), 0, 0, 0, 0);
    bool const service = strncmp(profile, "service", 7) == 0;
    bool const inert = strcmp(profile, "service_inert") == 0;
    auto const expected_grants = service ? 3 | ker::abi::process::SERVICE_PRIVILEGES_LOCKED : 0;
    bool const restrictions = grants == expected_grants && ceiling == (inert ? 0 : ker::abi::process::CREDENTIAL_AUTHORITY_ALL);
    bool const secure = ur != ue || gr != ge || (service && !inert);
    bool const auxv = getauxval(AT_UID) == ur && getauxval(AT_EUID) == ue && getauxval(AT_GID) == gr && getauxval(AT_EGID) == ge &&
                      getauxval(AT_SECURE) == static_cast<unsigned long>(secure);
    auto* marker = getenv("WOS_EXEC_OBSERVATION");
    auto* secure_marker = secure_getenv("WOS_EXEC_OBSERVATION");
    bool const environment = marker && strcmp(marker, "benign-marker") == 0 &&
                             (secure ? secure_marker == nullptr : secure_marker && strcmp(secure_marker, marker) == 0);
    printf("AUXV host=%s ids=%d secure=%lu expected_secure=%d marker=%s service=%lld ceiling=%lld\n", host, auxv, getauxval(AT_SECURE),
           secure, secure_marker ? "visible" : "hidden", static_cast<long long>(grants), static_cast<long long>(ceiling));
    printf("%s host=%s pid=%ld uid=%u/%u/%u gid=%u/%u/%u expected=%u/%u saved_gid_retry=%d/%d saved_uid_retry=%d/%d\n",
           identity && discarded && restrictions && auxv && environment ? "PASS" : "FAIL", host, static_cast<long>(getpid()), ur, ue, us,
           gr, ge, gs, eu, eg, group_result, group_error, user_result, user_error);
    return identity && discarded && restrictions && auxv && environment ? 0 : 1;
}
auto execute_seed(char** argv, bool spawn = false) -> int {
    if (setenv("WOS_EXEC_OBSERVATION", "benign-marker", 1) != 0) return 11;
    bool const local = strcmp(argv[2], "local") == 0;
    if (ker::process::setwkitarget(local ? nullptr : argv[2], local ? 0 : strlen(argv[2]),
                                   local ? ker::process::WKI_TARGET_FLAG_LOCAL : ker::process::WKI_TARGET_FLAG_STRICT) != 0)
        return 6;
    char flag[] = "--report";
    char* args[]{argv[3], flag, argv[5], argv[6], argv[7], argv[4], nullptr};
    if (spawn) {
        pid_t child{};
        int const error = posix_spawn(&child, argv[3], nullptr, nullptr, args, environ);
        if (error) {
            printf("SPAWN-ERROR errno=%d\n", error);
            return 12;
        }
        int status{};
        pid_t waited;
        do {
            waited = waitpid(child, &status, 0);
        } while (waited < 0 && errno == EINTR);
        bool const reaped = waited == child && WIFEXITED(status);
        printf("SPAWN-COMPLETE child=%ld reaped=%d status=%d\n", static_cast<long>(child), reaped, status);
        return reaped ? WEXITSTATUS(status) : 13;
    }
    execv(argv[3], args);
    printf("EXEC-ERROR errno=%d\n", errno);
    return 7;
}
}  // namespace

int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    alarm(45);
    if (argc == 6 && strcmp(argv[1], "--report") == 0)
        return report(static_cast<unsigned>(strtoul(argv[2], nullptr, 10)), static_cast<unsigned>(strtoul(argv[3], nullptr, 10)), argv[4],
                      argv[5]);
    if ((argc == 8 || argc == 9) && strcmp(argv[1], "--seed") == 0) {
        // A local set-ID exec seeds distinct saved IDs using WOS's public ABI.
        bool const service = strncmp(argv[4], "service", 7) == 0;
        if (getuid() != 1002 || geteuid() != (service ? 1002 : 1000) || getgid() != 2002 || getegid() != (service ? 2002 : 2000)) return 3;
        if (!service && (setegid(2002) || seteuid(1002))) return 4;
        if (strcmp(argv[4], "nnp") == 0 && ker::process::prctl(38, 1, 0, 0, 0) != 0) return 5;
        if (argc == 9 && strcmp(argv[8], "worker") == 0) {
            pthread_t worker;
            if (pthread_create(
                    &worker, nullptr,
                    [](void* context) -> void* {
                        return reinterpret_cast<void*>(static_cast<intptr_t>(execute_seed(static_cast<char**>(context))));
                    },
                    argv) != 0)
                return 9;
            void* result{};
            return pthread_join(worker, &result) == 0 ? static_cast<int>(reinterpret_cast<intptr_t>(result)) : 10;
        }
        return execute_seed(argv, argc == 9 && strcmp(argv[8], "spawn") == 0);
    }
    if ((argc != 8 && argc != 9) || geteuid() != 0) return 2;
    // launcher target image nnp expected-euid expected-egid expected-host seed
    pid_t child = fork();
    if (child == 0) {
        if (strncmp(argv[3], "service", 7) == 0 &&
            (ker::process::prctl(38, 1, 0, 0, 0) ||
             ker::process::prctl(static_cast<int>(ker::abi::process::CredentialControl::SET_SERVICE_PRIVILEGES), 3, 0, 0, 0)))
            _exit(5);
        if (setgroups(0, nullptr) || setgid(2002) || setuid(1002)) _exit(3);
        if (strcmp(argv[3], "service_inert") == 0 &&
            ker::process::prctl(static_cast<int>(ker::abi::process::CredentialControl::DROP_AUTHORITY_CEILING), 0, 0, 0, 0))
            _exit(6);
        char flag[] = "--seed";
        char* args[]{argv[7], flag, argv[1], argv[2], argv[3], argv[4], argv[5], argv[6], argc == 9 ? argv[8] : nullptr, nullptr};
        execv(argv[7], args);
        printf("SEED-EXEC-ERROR errno=%d\n", errno);
        _exit(4);
    }
    if (child < 0) return 7;
    int status{};
    pid_t waited;
    do {
        waited = waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    bool const reaped = waited == child && WIFEXITED(status);
    printf("COMPLETE child=%ld reaped=%d status=%d\n", static_cast<long>(child), reaped, status);
    return reaped ? WEXITSTATUS(status) : 8;
}
