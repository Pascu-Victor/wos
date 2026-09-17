#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/process.h>
#include <sys/wait.h>
#include <unistd.h>

#include <initializer_list>

struct CallerState {
    uid_t uid[3]{};
    gid_t gid[3]{};
    gid_t groups[8]{};
    int group_count = -1;
    int64_t no_new_privileges = -1;
    int64_t ceiling = -1;
    int64_t grants = -1;
    int64_t dumpable = -1;
    pid_t pid = -1;
    char executable[512]{};

    bool capture() {
        using Control = ker::abi::process::CredentialControl;
        group_count = getgroups(8, groups);
        no_new_privileges = ker::process::prctl(static_cast<int>(Control::GET_NO_NEW_PRIVILEGES), 0, 0, 0, 0);
        ceiling = ker::process::prctl(static_cast<int>(Control::GET_AUTHORITY_CEILING), 0, 0, 0, 0);
        grants = ker::process::prctl(static_cast<int>(Control::GET_SERVICE_PRIVILEGES), 0, 0, 0, 0);
        dumpable = ker::process::prctl(3, 0, 0, 0, 0);
        pid = getpid();
        auto length = readlink("/proc/self/exe", executable, sizeof(executable) - 1);
        return getresuid(&uid[0], &uid[1], &uid[2]) == 0 && getresgid(&gid[0], &gid[1], &gid[2]) == 0 && group_count >= 0 &&
               no_new_privileges >= 0 && ceiling >= 0 && grants >= 0 && dumpable >= 0 && length > 0 && length < sizeof(executable) - 1;
    }
    bool operator==(const CallerState& other) const {
        return !memcmp(uid, other.uid, sizeof(uid)) && !memcmp(gid, other.gid, sizeof(gid)) && group_count == other.group_count &&
               !memcmp(groups, other.groups, sizeof(groups)) && no_new_privileges == other.no_new_privileges && ceiling == other.ceiling &&
               grants == other.grants && dumpable == other.dumpable && pid == other.pid && !strcmp(executable, other.executable);
    }
};

struct Attempt {
    const char* self;
    const char* bad;
    bool fallback;
    bool worker;
    unsigned uid;
};

static void* attempt(void* opaque) {
    auto& a = *static_cast<Attempt*>(opaque);
    const char* target = a.fallback ? "goal19-absent-one-shot" : "";
    uint32_t flags = ker::process::WKI_TARGET_FLAG_ONESHOT | (a.fallback ? 0 : ker::process::WKI_TARGET_FLAG_LOCAL);
    if (ker::process::setwkitarget(target, strlen(target), flags) != 0) return reinterpret_cast<void*>(2);
    char before_target[256]{}, after_target[256]{};
    uint32_t before_flags{}, after_flags{};
    CallerState before{}, after{};
    int pipefd[2];
    if (!before.capture() || ker::process::getwkitarget(before_target, sizeof(before_target), &before_flags) < 0 || pipe(pipefd))
        return reinterpret_cast<void*>(2);
    bool prepared =
        fcntl(pipefd[0], F_SETFD, FD_CLOEXEC) == 0 && fcntl(pipefd[1], F_SETFD, FD_CLOEXEC) == 0 && write(pipefd[1], "S", 1) == 1;
    if (!prepared) {
        close(pipefd[0]);
        close(pipefd[1]);
        return reinterpret_cast<void*>(2);
    }
    char* denied_args[]{const_cast<char*>(a.bad), nullptr};
    execv(a.bad, denied_args);
    int error = errno;
    bool same = after.capture() && before == after;
    bool routing = ker::process::getwkitarget(after_target, sizeof(after_target), &after_flags) >= 0 && before_flags == after_flags &&
                   !strcmp(before_target, after_target);
    bool descriptors = fcntl(pipefd[0], F_GETFD) == FD_CLOEXEC && fcntl(pipefd[1], F_GETFD) == FD_CLOEXEC;
    close(pipefd[1]);
    char byte{};
    descriptors = read(pipefd[0], &byte, 1) == 1 && byte == 'S' && descriptors;
    close(pipefd[0]);
    printf("DENIAL uid=%u worker=%d fallback=%d errno=%d credentials=%d descriptors=%d routing=%d flags=%u/%u target=%s/%s\n", a.uid,
           a.worker, a.fallback, error, same, descriptors, routing, before_flags, after_flags, before_target, after_target);
    fflush(stdout);
    if (error != EACCES || !same || !descriptors || !routing) return reinterpret_cast<void*>(2);
    char probe[] = "--probe";
    char* args[]{const_cast<char*>(a.self), probe, nullptr};
    execv(a.self, args);
    return reinterpret_cast<void*>(2);
}

static bool wait_child(pid_t child) {
    for (unsigned i = 0; i < 20000; ++i) {
        int status{};
        auto result = waitpid(child, &status, WNOHANG);
        if (result == child) return WIFEXITED(status) && WEXITSTATUS(status) == 23;
        if (result < 0 && errno != EINTR) return false;
        usleep(1000);
    }
    kill(child, SIGKILL);
    for (unsigned i = 0; i < 2000; ++i) {
        int status{};
        if (waitpid(child, &status, WNOHANG) == child) return false;
        usleep(1000);
    }
    puts("FAIL: timed-out child was not reaped");
    return false;
}

auto main(int argc, char** argv) -> int {
    if (argc == 2 && !strcmp(argv[1], "--probe")) {
        char target[256]{};
        uint32_t flags{};
        bool ok = ker::process::getwkitarget(target, sizeof(target), &flags) >= 0 && target[0] == '\0' &&
                  flags == ker::process::WKI_TARGET_FLAG_LOCAL;
        printf("PROBE local=%d flags=%u target=%s\n", ok, flags, target);
        return ok ? 23 : 2;
    }
    if (argc != 2 || getuid() != 0 || ker::process::setwkitarget("", 0, ker::process::WKI_TARGET_FLAG_LOCAL)) return 2;
    unsigned failures = 0, cases = 0;
    for (unsigned uid : {0U, 1000U})
        for (bool worker : {false, true})
            for (bool fallback : {false, true}) {
                fflush(stdout);
                pid_t child = fork();
                if (child < 0) return 2;
                if (child == 0) {
                    if (setgroups(0, nullptr) || (uid && (setgid(uid) || setuid(uid)))) _exit(2);
                    Attempt a{argv[0], argv[1], fallback, worker, uid};
                    if (worker) {
                        // A worker's successful one-shot choice must reach the canonical owner.
                        if (ker::process::setwkitarget("", 0, ker::process::WKI_TARGET_FLAG_BALANCED)) _exit(2);
                        pthread_t thread{};
                        void* result{};
                        if (pthread_create(&thread, nullptr, attempt, &a) || pthread_join(thread, &result)) _exit(2);
                    } else
                        attempt(&a);
                    _exit(2);
                }
                ++cases;
                bool ok = wait_child(child);
                if (!ok) ++failures;
                printf("CASE uid=%u worker=%d fallback=%d passed=%d\n", uid, worker, fallback, ok);
            }
    printf("RESULT cases=%u failures=%u\n", cases, failures);
    return failures ? 2 : 0;
}
