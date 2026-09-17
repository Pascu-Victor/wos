// A worker created before chdir must spawn from the shared process cwd.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <initializer_list>

namespace {
struct Request {
    const char* image;
    const char* peer;
    unsigned profile;
    bool remote;
    std::atomic<bool> ready{false}, go{false};
    int result = 1;
};
int reap(pid_t child) {
    int status = 0;
    pid_t result;
    do {
        result = waitpid(child, &status, 0);
    } while (result < 0 && errno == EINTR);
    return result == child && WIFEXITED(status) ? WEXITSTATUS(status) : 90;
}
int spawn(Request& request) {
    int64_t target = request.remote ? ker::process::setwkitarget(request.peer, strlen(request.peer), ker::process::WKI_TARGET_FLAG_STRICT)
                                    : ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL);
    if (target) return 91;
    char profile[16];
    snprintf(profile, sizeof(profile), "%u", request.profile);
    const char* args[] = {request.image, "--payload", profile, nullptr};
    ker::abi::process::SpawnOptions options{};
    options.size = sizeof(options);
    options.version = ker::abi::process::SPAWN_OPTIONS_VERSION;
    // Use the kernel spawn ABI directly so libc fallback cannot hide which
    // parent Task supplies the child's placement context.
    int64_t child = static_cast<int64_t>(ker::process::spawn(request.image, args, nullptr, &options));
    if (child <= 0) {
        printf("FAIL spawn result=%ld\n", child);
        return 92;
    }
    return reap(static_cast<pid_t>(child));
}
void* worker(void* opaque) {
    auto& request = *static_cast<Request*>(opaque);
    request.ready.store(true, std::memory_order_release);
    while (!request.go.load(std::memory_order_acquire)) usleep(1000);
    request.result = spawn(request);
    return nullptr;
}
int payload(unsigned profile) {
    char data[5]{}, cwd[512]{};
    int fd = open("file", O_RDONLY), error = errno;
    ssize_t count = fd >= 0 ? read(fd, data, 4) : -1;
    if (fd >= 0) close(fd);
    bool identity = getuid() == (profile ? 100 : 0) && geteuid() == getuid() && getgid() == (profile ? 100 : 0) && getegid() == getgid();
    bool ok = identity && count == 4 && !memcmp(data, "seed", 4);
    printf("%s payload identity=%d cwd=%s data=%s open_errno=%d\n", ok ? "PASS" : "FAIL", identity,
           getcwd(cwd, sizeof(cwd)) ? cwd : "<error>", data, error);
    return ok ? 0 : 1;
}
}  // namespace

int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc == 3 && !strcmp(argv[1], "--payload")) return payload(static_cast<unsigned>(atoi(argv[2])));
    if (argc != 3 || ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL) ||
        ker::abi::vfs::wki_rule_add_vfs("/", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL) || chdir("/"))
        return 2;
    char file[512];
    if (snprintf(file, sizeof(file), "%s/file", argv[2]) >= static_cast<int>(sizeof(file)) || mkdir(argv[2], 0755)) return 3;
    int fd = open(file, O_WRONLY | O_CREAT | O_EXCL, 0644);
    bool setup = fd >= 0 && write(fd, "seed", 4) == 4;
    if (fd >= 0) setup &= close(fd) == 0;
    unsigned failed = 0, checks = 0;
    for (unsigned profile : {0U, 1U}) {
        for (bool threaded : {false, true}) {
            for (bool remote : {false, true}) {
                if (!setup) continue;
                pid_t child = fork();
                if (child == 0) {
                    if (profile && (setgroups(0, nullptr) || setgid(100) || setuid(100))) _exit(93);
                    Request request{argv[0], argv[1], profile, remote};
                    pthread_t thread;
                    if (threaded) {
                        if (pthread_create(&thread, nullptr, worker, &request)) _exit(94);
                        while (!request.ready.load(std::memory_order_acquire)) usleep(1000);
                    }
                    // The worker's legacy Task-local copy is still "/".
                    if (chdir(argv[2])) _exit(95);
                    if (threaded) {
                        request.go.store(true, std::memory_order_release);
                        if (pthread_join(thread, nullptr)) _exit(96);
                        _exit(request.result);
                    }
                    _exit(spawn(request));
                }
                int result = child > 0 ? reap(child) : 97;
                printf("%s profile=%u worker=%d remote=%d status=%d\n", result ? "FAIL" : "PASS", profile, threaded, remote, result);
                ++checks;
                failed += result != 0;
            }
        }
    }
    bool cleaned = unlink(file) == 0 && rmdir(argv[2]) == 0;
    bool ok = setup && checks == 8 && failed == 0 && cleaned;
    printf("%s spawn cwd checks=%u failed=%u cleanup=%d\n", ok ? "PASS" : "FAIL", checks, failed, cleaned);
    return ok ? 0 : 1;
}
