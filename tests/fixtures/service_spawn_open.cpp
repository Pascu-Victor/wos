// Ordinary posix_spawn OPEN/action-order regression for a disposable WOS VM.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <signal.h>
#include <spawn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/process.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;
namespace {
constexpr int TARGET = 8;
bool raw_mode;
bool fallback_mode;
int reap(pid_t pid) {
    int status{};
    for (unsigned i = 0; pid > 0 && i < 10000; ++i) {
        pid_t result = waitpid(pid, &status, WNOHANG);
        if (result == pid) return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
        if (result < 0 && errno != EINTR) break;
        usleep(1000);
    }
    if (pid > 0) {
        kill(pid, SIGKILL);
        for (unsigned i = 0; i < 1000; ++i) {
            if (waitpid(pid, &status, WNOHANG) == pid) break;
            usleep(1000);
        }
    }
    return 90;
}
bool reset(const char* path, mode_t mode) {
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, mode);
    if (fd < 0) return false;
    bool ok = write(fd, "keep", 4) == 4 && fchmod(fd, mode) == 0;
    return close(fd) == 0 && ok;
}
bool intact(const char* path) {
    int fd = open(path, O_RDONLY);
    char bytes[5]{};
    bool ok = fd >= 0 && read(fd, bytes, sizeof(bytes)) == 4 && !strcmp(bytes, "keep");
    if (fd >= 0) close(fd);
    return ok;
}
int child(const char* image, const char* base, unsigned which, int expected) {
    if (setgroups(0, nullptr) || setgid(2001) || setuid(1000)) return 91;
    char path[512], descriptor[24], kind[24];
    const char* suffix = "/public/data";
    if (which == 1 || which == 3) suffix = "/private/../public/data";
    if (which == 4 || which == 19 || which == 20 || which == 22 || which == 23) suffix = "/public/new";
    if (which == 5) suffix = "/private/../public/new";
    if (which == 6) suffix = "/public/missing";
    if (which == 7) suffix = "/public/restricted";
    if (which == 8) suffix = "/public/link";
    snprintf(path, sizeof(path), "%s%s", base, suffix);
    if (which == 14 || which == 15) snprintf(path, sizeof(path), "/proc/self/fd/10");
    int flags = O_RDONLY;
    if (which == 2 || which == 3) flags = O_WRONLY | O_TRUNC;
    if (which == 4 || which == 5 || which == 19 || which == 20 || which == 22 || which == 23) flags = O_WRONLY | O_CREAT | O_EXCL;
    if (which == 8) flags |= O_NOFOLLOW;
    if (which == 9 || which == 13) flags |= O_CLOEXEC;
    int parent_fd = open("/dev/null", O_RDONLY);
    if (parent_fd < 0 || dup2(parent_fd, TARGET) != TARGET) return 92;
    if (parent_fd != TARGET) close(parent_fd);
    if (which == 14 || which == 15) {
        char source[512];
        snprintf(source, sizeof(source), "%s/public/data", base);
        int input = open(source, O_RDONLY);
        if (input < 0 || dup2(input, 10) != 10) return 92;
        if (input != 10) close(input);
    }
    if (which == 11 || which == 12 || which == 18 || which == 22) {
        rlimit limit{16, 16};
        if (setrlimit(RLIMIT_NOFILE, &limit)) return 93;
        if (which == 11) {
            while (open("/dev/null", O_RDONLY | O_CLOEXEC) >= 0) {
            }
            if (errno != EMFILE) return 94;
        }
    }
    posix_spawn_file_actions_t actions;
    if (posix_spawn_file_actions_init(&actions)) return 95;
    int fd = which == 12 || which == 18 || which == 22 ? 16 : which == 23 ? -1 : which == 17 ? 31 : which == 21 ? 4 : TARGET;
    int error = which == 15 ? posix_spawn_file_actions_addclose(&actions, 10) : 0;
    posix_spawnattr_t attr;
    if (posix_spawnattr_init(&attr)) return 95;
    // Empty signal-default attributes force the generic fork/exec path.
    if (fallback_mode && posix_spawnattr_setflags(&attr, POSIX_SPAWN_SETSIGDEF)) return 95;
    if (!error && (which == 16 || which == 17 || which == 18 || which == 20))
        error = posix_spawn_file_actions_adddup2(&actions, which == 18 ? TARGET : 31, fd);
    if (!error && (which < 16 || which == 19 || which == 20 || which == 21 || which == 22 || which == 23))
        error = posix_spawn_file_actions_addopen(&actions, fd, path, flags, 0644);
    if (!error && which == 19) error = posix_spawn_file_actions_adddup2(&actions, 31, TARGET + 1);
    if (!error && which == 10) error = posix_spawn_file_actions_adddup2(&actions, TARGET, TARGET + 1);
    if (!error && which == 10) error = posix_spawn_file_actions_addclose(&actions, TARGET);
    if (!error && which == 13) error = posix_spawn_file_actions_adddup2(&actions, TARGET, TARGET);
    snprintf(descriptor, sizeof(descriptor), "%d", which == 10 ? TARGET + 1 : fd);
    snprintf(kind, sizeof(kind), "%u", which);
    char flag[] = "--payload";
    char* args[] = {const_cast<char*>(image), flag, descriptor, kind, nullptr};
    pid_t pid = -1;
    if (!error && raw_mode) {
        ker::abi::process::SpawnFdAction raw[3]{};
        unsigned count = 0;
        if (which == 15) raw[count++] = {.type = static_cast<unsigned>(ker::abi::process::SpawnFdActionType::CLOSE), .fd = 10};
        if (which == 16 || which == 17 || which == 18 || which == 20)
            raw[count++] = {
                .type = static_cast<unsigned>(ker::abi::process::SpawnFdActionType::DUP2), .fd = fd, .srcfd = which == 18 ? TARGET : 31};
        if (which < 16 || which == 19 || which == 20 || which == 21 || which == 22 || which == 23)
            raw[count++] = {.type = static_cast<unsigned>(ker::abi::process::SpawnFdActionType::OPEN),
                            .fd = fd,
                            .oflag = flags,
                            .mode = 0644,
                            .path = path};
        if (which == 19)
            raw[count++] = {.type = static_cast<unsigned>(ker::abi::process::SpawnFdActionType::DUP2), .fd = TARGET + 1, .srcfd = 31};
        if (which == 10 || which == 13)
            raw[count++] = {.type = static_cast<unsigned>(ker::abi::process::SpawnFdActionType::DUP2),
                            .fd = which == 10 ? TARGET + 1 : TARGET,
                            .srcfd = TARGET};
        if (which == 10) raw[count++] = {.type = static_cast<unsigned>(ker::abi::process::SpawnFdActionType::CLOSE), .fd = TARGET};
        ker::abi::process::SpawnOptions options{
            .size = sizeof(options), .version = ker::abi::process::SPAWN_OPTIONS_VERSION, .actions = raw, .action_count = count};
        auto result = static_cast<int64_t>(
            ker::process::spawn(image, const_cast<const char* const*>(args), const_cast<const char* const*>(environ), &options));
        error = result < 0 ? static_cast<int>(-result) : result == 0 ? EAGAIN : 0;
        if (!error) pid = static_cast<pid_t>(result);
    } else if (!error)
        error = posix_spawn(&pid, image, &actions, &attr, args, environ);
    posix_spawnattr_destroy(&attr);
    posix_spawn_file_actions_destroy(&actions);
    int status = error ? 0 : reap(pid);
    char byte{};
    uid_t real{}, effective{}, saved{};
    gid_t group_real{}, group_effective{}, group_saved{};
    bool parent_unchanged = (!error || pid == -1) && getresuid(&real, &effective, &saved) == 0 && real == 1000 && effective == real &&
                            saved == real && getresgid(&group_real, &group_effective, &group_saved) == 0 && group_real == 2001 &&
                            group_effective == group_real && group_saved == group_real && getgroups(0, nullptr) == 0 &&
                            fcntl(TARGET, F_GETFD) == 0 && read(TARGET, &byte, 1) == 0;
    if (error != expected || status || !parent_unchanged)
        printf("detail case=%u error=%d expected=%d child=%d parent=%d\n", which, error, expected, status, parent_unchanged);
    return error == expected && !status && parent_unchanged ? 0 : 96;
}
}  // namespace
int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc == 4 && !strcmp(argv[1], "--payload")) {
        int fd = atoi(argv[2]), which = atoi(argv[3]);
        uid_t r{}, e{}, s{};
        if (getresuid(&r, &e, &s) || r != 1000 || e != r || s != r) return 97;
        if (which == 9) {
            errno = 0;
            return fcntl(fd, F_GETFD) == -1 && errno == EBADF ? 0 : 98;
        }
        if (which == 2 || which == 4) return write(fd, "new", 3) == 3 ? 0 : 99;
        char bytes[5]{};
        return fcntl(fd, F_GETFD) == 0 && read(fd, bytes, sizeof(bytes)) == 4 && !strcmp(bytes, "keep") ? 0 : 100;
    }
    raw_mode = argc == 3 && !strcmp(argv[2], "--raw");
    fallback_mode = argc == 3 && !strcmp(argv[2], "--fallback");
    if ((argc != 2 && !raw_mode && !fallback_mode) || ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL)) return 2;
    char public_dir[512], private_dir[512], data[512], restricted[512], created[512], link[512];
    snprintf(public_dir, sizeof(public_dir), "%s/public", argv[1]);
    snprintf(private_dir, sizeof(private_dir), "%s/private", argv[1]);
    snprintf(data, sizeof(data), "%s/data", public_dir);
    snprintf(restricted, sizeof(restricted), "%s/restricted", public_dir);
    snprintf(created, sizeof(created), "%s/new", public_dir);
    snprintf(link, sizeof(link), "%s/link", public_dir);
    bool setup = mkdir(argv[1], 0755) == 0 && mkdir(public_dir, 0777) == 0 && chmod(public_dir, 0777) == 0 &&
                 mkdir(private_dir, 0700) == 0 && reset(restricted, 0600) && symlink("data", link) == 0;
    int errors[] = {0,     EACCES, 0, EACCES, 0,     EACCES, ENOENT, EACCES, ELOOP, 0, 0,     0,
                    EBADF, 0,      0, ENOENT, EBADF, EBADF,  EBADF,  EBADF,  EBADF, 0, EBADF, EBADF};
    if (fallback_mode) errors[11] = EMFILE;  // Parent error pipe cannot be allocated in a full table.
    unsigned expected_cases = 24;
    unsigned cases = 0, failed = 0;
    for (unsigned which = 0; setup && which < expected_cases; ++which) {
        if (!reset(data, 0666)) {
            setup = false;
            break;
        }
        unlink(created);
        pid_t pid = fork();
        if (!pid) _exit(child(argv[0], argv[1], which, errors[which]));
        int status = reap(pid);
        struct stat st{};
        bool state = which == 2 ? stat(data, &st) == 0 && st.st_size == 3 : intact(data);
        if (which == 19)
            state &= stat(created, &st) == 0 && st.st_uid == 1000 && st.st_gid == 2001 && st.st_size == 0;
        else
            state &= which == 4 ? stat(created, &st) == 0 && st.st_uid == 1000 && st.st_gid == 2001 && st.st_size == 3
                                : stat(created, &st) < 0 && errno == ENOENT;
        printf("%s case=%u expected=%d status=%d state=%d\n", !status && state ? "PASS" : "FAIL", which, errors[which], status, state);
        failed += status != 0 || !state;
        ++cases;
    }
    bool clean = true;
    const char* files[] = {link, created, data, restricted};
    const char* directories[] = {private_dir, public_dir, argv[1]};
    for (const char* p : files)
        if (unlink(p) && errno != ENOENT) clean = false;
    for (const char* p : directories)
        if (rmdir(p) && errno != ENOENT) clean = false;
    printf("spawn OPEN cleanup %s\n", clean ? "completed" : "FAILED");
    bool ok = setup && clean && cases == expected_cases && !failed;
    printf("%s: %u spawn OPEN cases\n", ok ? "PASS" : "FAIL", cases);
    return ok ? 0 : 1;
}
