#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
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
    int ready;
    int go;
};
static void* attempt(void* opaque) {
    auto& a = *static_cast<Attempt*>(opaque);
    if (ker::process::setwkitarget("wos-3", 5, ker::process::WKI_TARGET_FLAG_STRICT)) return reinterpret_cast<void*>(2);
    CallerState before{};
    if (!before.capture() || fcntl(a.ready, F_SETFD, FD_CLOEXEC) || write(a.ready, "R", 1) != 1) return reinterpret_cast<void*>(2);
    char go{};
    if (read(a.go, &go, 1) != 1 || go != 'G') return reinterpret_cast<void*>(2);
    close(a.go);
    for (unsigned i = 0; i < 32; ++i) {
        char* args[]{const_cast<char*>(a.path), nullptr};
        execv(a.path, args);
        int error = errno;
        CallerState after{};
        if (error != EACCES || !after.capture() || !(before == after) || fcntl(a.ready, F_GETFD) != FD_CLOEXEC)
            return reinterpret_cast<void*>(2);
    }
    return write(a.ready, "D", 1) == 1 ? nullptr : reinterpret_cast<void*>(2);
}
static uint64_t now_ms() {
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000ULL + ts.tv_nsec / 1000000;
}
auto main(int argc, char** argv) -> int {
    if (argc != 2 || getuid() != 0 || ker::process::setwkitarget("", 0, ker::process::WKI_TARGET_FLAG_LOCAL)) return 2;
    unsigned failures = 0;
    for (unsigned uid : {0U, 1000U})
        for (bool worker : {false, true}) {
            int report[2], go[2];
            if (pipe(report) || pipe(go)) return 2;
            fflush(stdout);
            pid_t child = fork();
            if (child < 0) return 2;
            if (child == 0) {
                close(report[0]);
                close(go[1]);
                if (setgroups(0, nullptr) || (uid && (setgid(uid) || setuid(uid)))) _exit(2);
                Attempt a{argv[1], report[1], go[0]};
                void* result{};
                if (worker) {
                    pthread_t thread{};
                    if (pthread_create(&thread, nullptr, attempt, &a) || pthread_join(thread, &result)) _exit(2);
                } else
                    result = attempt(&a);
                _exit(result ? 2 : 0);
            }
            close(report[1]);
            close(go[0]);
            char ready{};
            bool armed = read(report[0], &ready, 1) == 1 && ready == 'R';
            char link[64]{}, original[512]{};
            snprintf(link, sizeof(link), "/proc/%ld/exe", static_cast<long>(child));
            auto initial = readlink(link, original, sizeof(original) - 1);
            armed = initial > 0 && armed;
            if (write(go[1], "G", 1) != 1) armed = false;
            close(go[1]);
            unsigned old_count = 0, candidate = 0, other = 0, errors = 0;
            bool reaped = false;
            int status{};
            uint64_t deadline = now_ms() + 20000;
            while (now_ms() < deadline) {
                char value[512]{};
                auto length = readlink(link, value, sizeof(value) - 1);
                if (length > 0) {
                    if (!strcmp(value, original))
                        ++old_count;
                    else if (!strcmp(value, argv[1]))
                        ++candidate;
                    else
                        ++other;
                } else if (errno != ENOENT && errno != ESRCH)
                    ++errors;
                auto waited = waitpid(child, &status, WNOHANG);
                if (waited == child) {
                    reaped = true;
                    break;
                }
                if (waited < 0 && errno != EINTR) break;
                usleep(100);
            }
            if (!reaped) {
                kill(child, SIGKILL);
                for (unsigned i = 0; i < 2000; ++i) {
                    if (waitpid(child, &status, WNOHANG) == child) {
                        reaped = true;
                        break;
                    }
                    usleep(1000);
                }
            }
            char done{};
            bool completed = reaped && WIFEXITED(status) && WEXITSTATUS(status) == 0 && read(report[0], &done, 1) == 1 && done == 'D';
            close(report[0]);
            bool ok = armed && completed && old_count > 0 && candidate == 0 && other == 0 && errors == 0;
            printf("CASE uid=%u worker=%d armed=%d completed=%d reaped=%d old=%u candidate=%u other=%u errors=%u passed=%d\n", uid, worker,
                   armed, completed, reaped, old_count, candidate, other, errors, ok);
            if (!ok) ++failures;
        }
    printf("RESULT cases=4 attempts=128 failures=%u\n", failures);
    return failures ? 2 : 0;
}
