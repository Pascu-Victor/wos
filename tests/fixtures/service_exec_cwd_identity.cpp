// Preserve the admitted directory across local/remote main/worker exec.
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

#include <initializer_list>

namespace {
bool create(const char* path, const char* text) {
    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0) return false;
    bool const written = write(fd, text, 4) == 4;
    return close(fd) == 0 && written;
}
struct Execution {
    char* image;
    const char* peer;
    bool remote;
    unsigned profile;
};
void* execute(void* opaque) {
    auto& request = *static_cast<Execution*>(opaque);
    auto const result = request.remote
                            ? ker::process::setwkitarget(request.peer, strlen(request.peer), ker::process::WKI_TARGET_FLAG_STRICT)
                            : ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL);
    if (result) {
        printf("FAIL target admission result=%ld\n", result);
        _exit(4);
    }
    char mode[] = "--payload", profile[16];
    snprintf(profile, sizeof(profile), "%u", request.profile);
    char* args[] = {request.image, mode, profile, nullptr};
    execv(request.image, args);
    printf("FAIL exec refused errno=%d\n", errno);
    _exit(5);
}
bool run(Execution request, bool worker, const char* stage) {
    pid_t child = fork();
    if (child == 0) {
        if (request.profile && (setgroups(0, nullptr) || setgid(100) || setuid(100))) _exit(6);
        if (worker) {
            pthread_t thread;
            if (pthread_create(&thread, nullptr, execute, &request) || pthread_join(thread, nullptr)) _exit(7);
            _exit(8);
        }
        execute(&request);
        _exit(9);
    }
    int status = 0;
    pid_t reaped;
    do {
        reaped = child > 0 ? waitpid(child, &status, 0) : -1;
    } while (reaped < 0 && errno == EINTR);
    bool const ok = child > 0 && reaped == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
    printf("%s stage=%s profile=%u worker=%d remote=%d status=%d\n", ok ? "PASS" : "FAIL", stage, request.profile, worker, request.remote,
           status);
    return ok;
}
int payload(unsigned profile) {
    char cwd[512]{}, contents[5]{};
    int fd = open("file", O_RDONLY);
    int const open_error = errno;
    ssize_t const bytes = fd >= 0 ? read(fd, contents, 4) : -1;
    if (fd >= 0) close(fd);
    char* name = getcwd(cwd, sizeof(cwd));
    uid_t real, effective, saved;
    gid_t group_real, group_effective, group_saved;
    unsigned const expected = profile ? 100 : 0;
    bool const identity = getresuid(&real, &effective, &saved) == 0 && getresgid(&group_real, &group_effective, &group_saved) == 0 &&
                          real == expected && effective == expected && saved == expected && group_real == expected &&
                          group_effective == expected && group_saved == expected;
    bool const retained = bytes == 4 && !memcmp(contents, "seed", 4);
    printf("%s payload identity=%d retained=%d cwd=%s data=%s bytes=%ld open_errno=%d\n", identity && retained ? "PASS" : "FAIL", identity,
           retained, name ? cwd : "<error>", contents, bytes, open_error);
    return identity && retained ? 0 : 1;
}
}  // namespace

int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc == 3 && !strcmp(argv[1], "--payload")) return payload(static_cast<unsigned>(atoi(argv[2])));
    if (argc != 3 || ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL)) return 2;
    // Explicitly keep localized paths on the receiver, including its XFS
    // root. This avoids the default submitter route selecting another node.
    if (ker::abi::vfs::wki_rule_add_vfs("/", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL)) return 2;
    char original[512], moved[512], file[512], moved_file[512];
    if (snprintf(original, sizeof(original), "%s/original", argv[2]) >= static_cast<int>(sizeof(original)) ||
        snprintf(moved, sizeof(moved), "%s/moved", argv[2]) >= static_cast<int>(sizeof(moved)) ||
        snprintf(file, sizeof(file), "%s/file", original) >= static_cast<int>(sizeof(file)) ||
        snprintf(moved_file, sizeof(moved_file), "%s/file", moved) >= static_cast<int>(sizeof(moved_file)))
        return 2;
    bool const setup = mkdir(argv[2], 0755) == 0 && mkdir(original, 0755) == 0 && create(file, "seed") && chdir(original) == 0;
    unsigned checks = 0, failed = 0;
    auto check = [&](bool ok) {
        ++checks;
        failed += !ok;
    };
    if (setup) {
        // First prove the same image and remote directory work before rename.
        check(run({argv[0], argv[1], true, 0}, false, "before-rename"));
        bool const renamed = rename(original, moved) == 0 && mkdir(original, 0755) == 0 && create(file, "fake");
        if (renamed) {
            for (unsigned profile : {0U, 1U})
                for (bool worker : {false, true})
                    for (bool remote : {false, true}) check(run({argv[0], argv[1], remote, profile}, worker, "after-rename"));
        } else {
            printf("FAIL rename setup errno=%d\n", errno);
            ++failed;
        }
    } else {
        printf("FAIL initial setup errno=%d\n", errno);
        ++failed;
    }
    // Cleanup is best effort after partial setup as well as failed assertions.
    bool cleaned = chdir("/") == 0;
    for (auto const* path : {file, moved_file}) {
        int const result = unlink(path);
        cleaned &= result == 0 || errno == ENOENT;
    }
    for (auto const* path : {original, moved, argv[2]}) {
        int const result = rmdir(path);
        cleaned &= result == 0 || errno == ENOENT;
    }
    printf("%s cwd exec identity checks=%u failed=%u cleanup=%d\n", failed || !cleaned ? "FAIL" : "PASS", checks, failed, cleaned);
    return failed || !cleaned;
}
