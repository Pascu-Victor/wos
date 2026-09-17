#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/process.h>
#include <sys/wait.h>
#include <time.h>
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
    const char* path;
    const char* target;
    const char* uid;
    bool denied;
};
static void* attempt(void* opaque) {
    auto& a = *static_cast<Attempt*>(opaque);
    if (ker::process::setwkitarget(a.target, strlen(a.target), ker::process::WKI_TARGET_FLAG_STRICT)) return reinterpret_cast<void*>(2);
    CallerState before{};
    int pipefd[2];
    if (!before.capture() || pipe(pipefd)) return reinterpret_cast<void*>(2);
    bool configured =
        fcntl(pipefd[0], F_SETFD, FD_CLOEXEC) == 0 && fcntl(pipefd[1], F_SETFD, FD_CLOEXEC) == 0 && write(pipefd[1], "held", 4) == 4;
    if (!configured) {
        close(pipefd[0]);
        close(pipefd[1]);
        return reinterpret_cast<void*>(2);
    }
    char final[] = "--final";
    char* args[]{const_cast<char*>(a.path), final, const_cast<char*>(a.uid), const_cast<char*>(a.target), nullptr};
    execv(a.path, args);
    int error = errno;
    CallerState after{};
    char held[4]{};
    bool unchanged = after.capture() && before == after;
    bool descriptors = fcntl(pipefd[0], F_GETFD) == FD_CLOEXEC && fcntl(pipefd[1], F_GETFD) == FD_CLOEXEC &&
                       read(pipefd[0], held, sizeof(held)) == sizeof(held) && !memcmp(held, "held", sizeof(held));
    close(pipefd[0]);
    close(pipefd[1]);
    bool passed = a.denied && error == EACCES && unchanged && descriptors;
    printf("REFUSED pid=%ld uid=%u errno=%d state=%d descriptors=%d passed=%d\n", static_cast<long>(getpid()), getuid(), error, unchanged,
           descriptors, passed);
    return passed ? nullptr : reinterpret_cast<void*>(2);
}
static uint64_t now_ms() {
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000ULL + ts.tv_nsec / 1000000;
}
auto main(int argc, char** argv) -> int {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc == 4 && !strcmp(argv[1], "--final")) {
        char host[64]{};
        bool passed = gethostname(host, sizeof(host)) == 0 && !strcmp(host, argv[3]) && getuid() == static_cast<unsigned>(atoi(argv[2]));
        printf("FINAL host=%s pid=%ld uid=%u passed=%d\n", host, static_cast<long>(getpid()), getuid(), passed);
        return passed ? 23 : 2;
    }
    if (argc == 8 && !strcmp(argv[1], "--receiver")) {
        char host[64]{};
        if (gethostname(host, sizeof(host)) || strcmp(host, argv[7]) || getuid() != static_cast<unsigned>(atoi(argv[5]))) return 2;
        printf("RECEIVER host=%s pid=%ld uid=%u worker=%s\n", host, static_cast<long>(getpid()), getuid(), argv[6]);
        Attempt a{argv[2], argv[3], argv[5], !strcmp(argv[4], "deny")};
        void* result{};
        if (atoi(argv[6])) {
            pthread_t thread{};
            if (pthread_create(&thread, nullptr, attempt, &a) || pthread_join(thread, &result)) return 2;
        } else
            result = attempt(&a);
        return result ? 2 : 23;
    }
    if (argc != 5 || getuid() != 0 || (strcmp(argv[4], "deny") && strcmp(argv[4], "accept")) ||
        ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL))
        return 2;
    unsigned failed = 0;
    for (unsigned uid : {0U, 1000U})
        for (bool worker : {false, true}) {
            pid_t child = fork();
            if (child < 0) return 2;
            if (child == 0) {
                if (setgroups(0, nullptr) || (uid && (setgid(uid) || setuid(uid))) ||
                    ker::process::setwkitarget(argv[1], strlen(argv[1]), ker::process::WKI_TARGET_FLAG_STRICT))
                    _exit(2);
                char receiver[] = "--receiver", user[16]{}, thread[2]{worker ? '1' : '0', '\0'};
                snprintf(user, sizeof(user), "%u", uid);
                char image[512]{};
                if (snprintf(image, sizeof(image), "/wki/wos-0%s", argv[0]) >= static_cast<int>(sizeof(image))) _exit(2);
                char* args[]{image, receiver, argv[3], argv[2], argv[4], user, thread, argv[1], nullptr};
                execv(image, args);
                printf("INITIAL-EXEC-ERROR errno=%d\n", errno);
                _exit(2);
            }
            int status{};
            pid_t reaped{};
            uint64_t deadline = now_ms() + 20000;
            while (now_ms() < deadline) {
                reaped = waitpid(child, &status, WNOHANG);
                if (reaped == child || (reaped < 0 && errno != EINTR)) break;
                usleep(1000);
            }
            bool timely = reaped == child;
            if (!timely) {
                kill(child, SIGKILL);
                deadline = now_ms() + 3000;
                while (now_ms() < deadline) {
                    reaped = waitpid(child, &status, WNOHANG);
                    if (reaped == child || (reaped < 0 && errno != EINTR)) break;
                    usleep(1000);
                }
            }
            bool passed = timely && WIFEXITED(status) && WEXITSTATUS(status) == 23;
            printf("CASE uid=%u worker=%d mode=%s child=%ld timely=%d reaped=%d status=%d passed=%d\n", uid, worker, argv[4],
                   static_cast<long>(child), timely, reaped == child, status, passed);
            failed += !passed;
            if (reaped != child) return 3;
        }
    printf("RESULT cases=4 failed=%u\n", failed);
    return failed ? 1 : 0;
}
