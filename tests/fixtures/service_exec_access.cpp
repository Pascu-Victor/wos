// Disposable WOS VM regression for executable path authorization.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <pthread.h>
#include <signal.h>
#include <spawn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;
namespace {
struct Attempt {
    char* path;
    char** args;
    int error{};
};
void* execute(void* opaque) {
    auto& attempt = *static_cast<Attempt*>(opaque);
    execv(attempt.path, attempt.args);
    attempt.error = errno;
    return opaque;
}
int reap(pid_t pid) {
    int status = 0;
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
bool copy_image(const char* source, const char* target) {
    int input = open(source, O_RDONLY), output = open(target, O_CREAT | O_EXCL | O_WRONLY, 0755);
    bool ok = input >= 0 && output >= 0;
    char buffer[4096];
    while (ok) {
        ssize_t count = read(input, buffer, sizeof(buffer));
        if (!count) break;
        if (count < 0) {
            ok = false;
            break;
        }
        ssize_t done = 0;
        while (done < count) {
            ssize_t written = write(output, buffer + done, count - done);
            if (written <= 0) {
                ok = false;
                break;
            }
            done += written;
        }
    }
    if (input >= 0) close(input);
    if (output >= 0 && close(output)) ok = false;
    return ok;
}
bool identity() {
    uid_t real{}, effective{}, saved{};
    gid_t gr{}, ge{}, gs{};
    return getresuid(&real, &effective, &saved) == 0 && getresgid(&gr, &ge, &gs) == 0 && real == 1000 && effective == real &&
           saved == real && gr == 2001 && ge == gr && gs == gr;
}
int attempt(char* path, unsigned method, int expected, const char* cwd) {
    if ((cwd && chdir(cwd)) || setgroups(0, nullptr) || setgid(2001) || setuid(1000)) return 91;
    int secret = open("/dev/null", O_RDONLY | O_CLOEXEC);
    if (secret < 0) return 92;
    char before[512]{}, after[512]{}, descriptor[24];
    if (readlink("/proc/self/exe", before, sizeof(before) - 1) < 0) return 93;
    snprintf(descriptor, sizeof(descriptor), "%d", secret);
    char flag[] = "--payload";
    char* args[] = {path, flag, descriptor, nullptr};
    Attempt request{path, args};
    if (method == 2) {
        pid_t pid = -1;
        request.error = posix_spawn(&pid, path, nullptr, nullptr, args, environ);
        if (!request.error) {
            int const status = reap(pid);
            return expected ? 94 : status;
        }
    } else if (method == 1) {
        pthread_t thread;
        void* result = nullptr;
        if (pthread_create(&thread, nullptr, execute, &request) || pthread_join(thread, &result) || result != &request) return 95;
    } else {
        execute(&request);
    }
    bool unchanged = identity() && fcntl(secret, F_GETFD) == FD_CLOEXEC && readlink("/proc/self/exe", after, sizeof(after) - 1) >= 0 &&
                     !strcmp(before, after);
    close(secret);
    if (request.error != expected || !unchanged)
        printf("access error=%d expected=%d unchanged=%d path=%s\n", request.error, expected, unchanged, path);
    return expected && request.error == expected && unchanged ? 0 : 96;
}
}  // namespace
int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc == 3 && !strcmp(argv[1], "--payload")) {
        errno = 0;
        return identity() && fcntl(atoi(argv[2]), F_GETFD) == -1 && errno == EBADF ? 0 : 97;
    }
    if (argc != 2 || ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL)) return 2;
    char private_dir[512], public_dir[512], executable[512], link[512], private_image[512];
    snprintf(private_dir, sizeof(private_dir), "%s/private", argv[1]);
    snprintf(public_dir, sizeof(public_dir), "%s/public", argv[1]);
    snprintf(executable, sizeof(executable), "%s/image", public_dir);
    snprintf(private_image, sizeof(private_image), "%s/image", private_dir);
    snprintf(link, sizeof(link), "%s/link", argv[1]);
    bool setup = mkdir(argv[1], 0755) == 0 && mkdir(private_dir, 0700) == 0 && mkdir(public_dir, 0755) == 0 &&
                 copy_image(argv[0], executable) && copy_image(argv[0], private_image) && symlink("private/../public/image", link) == 0;
    int image_fd = setup ? open(executable, O_RDONLY) : -1;
    setup &= image_fd >= 0;
    const char* suffixes[] = {"/public/image",
                              "/private/../public/image",
                              "/private/./../public/image",
                              "/link",
                              "/public/../public/image",
                              "/public/missing",
                              nullptr,
                              nullptr,
                              "/private/image"};
    int expected[] = {0, EACCES, EACCES, EACCES, 0, ENOENT, 0, EACCES, EACCES};
    unsigned cases = 0, failed = 0;
    for (unsigned i = 0; setup && i < 9; ++i) {
        char path[512];
        if (i == 6)
            snprintf(path, sizeof(path), "/proc/self/fd/%d", image_fd);
        else if (i == 7)
            snprintf(path, sizeof(path), "../public/image");
        else
            snprintf(path, sizeof(path), "%s%s", argv[1], suffixes[i]);
        for (unsigned method = 0; method < 3; ++method) {
            pid_t pid = fork();
            if (!pid) _exit(attempt(path, method, expected[i], i == 7 ? private_dir : nullptr));
            int status = reap(pid);
            printf("%s access=%u method=%u expected=%d status=%d\n", status ? "FAIL" : "PASS", i, method, expected[i], status);
            failed += status != 0;
            ++cases;
        }
    }
    if (!setup) printf("setup failed errno=%d\n", errno);
    if (image_fd >= 0) close(image_fd);
    bool clean = true;
    const char* files[] = {link, private_image, executable};
    const char* directories[] = {private_dir, public_dir, argv[1]};
    for (const char* path : files)
        if (unlink(path) && errno != ENOENT) clean = false;
    for (const char* path : directories)
        if (rmdir(path) && errno != ENOENT) clean = false;
    printf("access fixture cleanup %s\n", clean ? "completed" : "FAILED");
    bool ok = setup && clean && cases == 27 && failed == 0;
    printf("%s: %u native exec access cases\n", ok ? "PASS" : "FAIL", cases);
    return ok ? 0 : 1;
}
