// Run only in a disposable WOS VM: mounts private TMPFS instances.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <spawn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/wait.h>
#include <unistd.h>

#include "mount_options.h"

extern char** environ;
namespace {
struct Attempt {
    char* image;
    char** args;
    int error;
};
struct Observer {
    int input = -1;
    int ready = 0;
    int stop = 0;
};
void* observe_old_authority(void* opaque) {
    auto& observer = *static_cast<Observer*>(opaque);
    __atomic_store_n(&observer.ready, 1, __ATOMIC_RELEASE);
    while (!__atomic_load_n(&observer.stop, __ATOMIC_ACQUIRE)) {
        if (geteuid() != 1000 || getegid() != 2001) _exit(110);
        if (observer.input >= 0) {
            char byte;
            if (read(observer.input, &byte, 1) == 0) break;
        } else {
            sched_yield();
        }
    }
    return nullptr;
}
void* replace(void* opaque) {
    auto* attempt = static_cast<Attempt*>(opaque);
    execv(attempt->image, attempt->args);
    attempt->error = errno;
    return opaque;
}
int wait_child(pid_t child) {
    int status = 0;
    for (unsigned i = 0; child > 0 && i < 10000; ++i) {
        pid_t result = waitpid(child, &status, WNOHANG);
        if (result == child) return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
        if (result < 0 && errno != EINTR) return 91;
        usleep(1000);
    }
    if (child > 0) {
        kill(child, SIGKILL);
        for (unsigned i = 0; i < 1000; ++i) {
            if (waitpid(child, &status, WNOHANG) == child) break;
            usleep(1000);
        }
    }
    return 92;
}
int payload(int argc, char** argv) {
    if (argc != 7) return 93;
    uid_t ur, ue, us;
    gid_t gr, ge, gs;
    char path[512]{};
    bool valid = getresuid(&ur, &ue, &us) == 0 && getresgid(&gr, &ge, &gs) == 0 && ur == 1000 &&
                 ue == static_cast<unsigned>(atoi(argv[2])) && us == ue && gr == 2001 && ge == static_cast<unsigned>(atoi(argv[3])) &&
                 gs == ge && ker::process::prctl(39, 0, 0, 0, 0) == atoi(argv[4]) &&
                 readlink("/proc/self/exe", path, sizeof(path) - 1) >= 0 && !strcmp(path, argv[0]);
    errno = 0;
    valid &= fcntl(atoi(argv[5]), F_GETFD) == -1 && errno == EBADF;
    valid &= atoi(argv[6]) == 0 || getpid() == atoi(argv[6]);
    if (!valid) printf("payload failure uid=%u/%u/%u gid=%u/%u/%u exe='%s'\n", ur, ue, us, gr, ge, gs, path);
    return valid ? 0 : 94;
}
int run_child(char* image, unsigned nosuid, unsigned nnp, unsigned bits, unsigned method) {
    if (setgroups(0, nullptr) || setgid(2001) || setuid(1000)) return 95;
    if (nnp && ker::process::prctl(38, 1, 0, 0, 0)) return 96;
    int secret = open("/dev/null", O_RDONLY | O_CLOEXEC);
    if (secret < 0) return 97;
    char uid[24], gid[24], no_new[24], fd[24], denied[24];
    bool suppress = nosuid || nnp;
    snprintf(uid, sizeof(uid), "%u", !suppress && (bits & 1) ? 0 : 1000);
    snprintf(gid, sizeof(gid), "%u", !suppress && (bits & 2) ? 0 : 2001);
    snprintf(no_new, sizeof(no_new), "%u", nnp);
    snprintf(fd, sizeof(fd), "%d", secret);
    snprintf(denied, sizeof(denied), "%ld", method == 2 ? 0L : static_cast<long>(getpid()));
    char flag[] = "--payload";
    char* args[] = {image, flag, uid, gid, no_new, fd, denied, nullptr};
    if (method == 2) {
        pid_t child = -1;
        int error = posix_spawn(&child, image, nullptr, nullptr, args, environ);
        return error ? error : wait_child(child);
    }
    Attempt attempt{image, args, 0};
    Observer observer;
    int pipe_fds[2]{-1, -1};
    pthread_t observer_thread{};
    bool const OBSERVER = method >= 3;
    if (OBSERVER) {
        if (method == 4) {
            if (pipe2(pipe_fds, O_CLOEXEC)) return 101;
            observer.input = pipe_fds[0];
        }
        if (pthread_create(&observer_thread, nullptr, observe_old_authority, &observer)) return 102;
        while (!__atomic_load_n(&observer.ready, __ATOMIC_ACQUIRE)) sched_yield();
    }
    char old_exe[512]{}, after_exe[512]{};
    if (readlink("/proc/self/exe", old_exe, sizeof(old_exe) - 1) < 0) return 98;
    if (method == 1 || method == 5) {
        pthread_t worker;
        void* joined = nullptr;
        if (pthread_create(&worker, nullptr, replace, &attempt) || pthread_join(worker, &joined) || joined != &attempt) return 99;
    } else {
        replace(&attempt);
    }
    if (OBSERVER) {
        __atomic_store_n(&observer.stop, 1, __ATOMIC_RELEASE);
        if (pipe_fds[1] >= 0) close(pipe_fds[1]);
        pthread_join(observer_thread, nullptr);
        if (pipe_fds[0] >= 0) close(pipe_fds[0]);
    }
    uid_t ur, ue, us;
    gid_t gr, ge, gs;
    bool unchanged = getresuid(&ur, &ue, &us) == 0 && getresgid(&gr, &ge, &gs) == 0 && ur == 1000 && ue == ur && us == ur && gr == 2001 &&
                     ge == gr && gs == gr && fcntl(secret, F_GETFD) == FD_CLOEXEC &&
                     readlink("/proc/self/exe", after_exe, sizeof(after_exe) - 1) >= 0 && !strcmp(old_exe, after_exe);
    printf("exec unexpectedly returned error=%d unchanged=%d\n", attempt.error, unchanged);
    close(secret);
    return 100;
}
bool copy_image(const char* source, const char* target, unsigned bits) {
    int in = open(source, O_RDONLY), out = open(target, O_CREAT | O_EXCL | O_WRONLY, 0700);
    bool ok = in >= 0 && out >= 0;
    char buffer[4096];
    while (ok) {
        ssize_t count = read(in, buffer, sizeof(buffer));
        if (!count) break;
        if (count < 0) {
            ok = false;
            break;
        }
        ssize_t done = 0;
        while (done < count) {
            ssize_t result = write(out, buffer + done, count - done);
            if (result <= 0) {
                ok = false;
                break;
            }
            done += result;
        }
    }
    if (in >= 0) close(in);
    if (out >= 0 && close(out)) ok = false;
    return ok && chown(target, 0, 0) == 0 && chmod(target, 0755 | (bits & 1 ? 04000 : 0) | (bits & 2 ? 02000 : 0)) == 0;
}
bool mount_options_visible(const char* target, unsigned nosuid, bool remote) {
    FILE* mounts = fopen("/proc/mounts", "r");
    if (!mounts) return false;
    bool found = false;
    char line[2048], source[512], path[512], type[32], options[128];
    for (unsigned i = 0; i < 1024 && fgets(line, sizeof(line), mounts); ++i) {
        if (sscanf(line, "%511s %511s %31s %127s", source, path, type, options) == 4 && !strcmp(path, target)) {
            found = !strcmp(type, remote ? "remote" : "tmpfs") && !strcmp(options, nosuid ? "rw,nosuid" : "rw");
            break;
        }
    }
    fclose(mounts);
    return found;
}
}  // namespace
int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc > 1 && !strcmp(argv[1], "--payload")) return payload(argc, argv);
    if (argc != 2 && argc != 4) return 2;
    if (ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL) != 0) return 3;
    bool const remote = argc == 4;
    char paths[2][512]{}, images[2][3][512]{};
    bool mounted[2]{}, made[2]{};
    unsigned failures = 0, cases = 0;
    bool setup = mkdir(argv[1], 0755) == 0;
    for (unsigned nosuid = 0; setup && nosuid < 2; ++nosuid) {
        snprintf(paths[nosuid], sizeof(paths[nosuid]), "%s/m%u", argv[1], nosuid);
        made[nosuid] = mkdir(paths[nosuid], 0755) == 0;
        wos::init::MountOptions parsed;
        bool policy = wos::init::parse_mount_options(nosuid ? "defaults,nosuid,size=32M" : "defaults,suid,size=32M", parsed) == 0;
        mounted[nosuid] = made[nosuid] && policy &&
                          mount(remote ? argv[2] : nullptr, paths[nosuid], remote ? "remote" : "tmpfs", parsed.flags,
                                remote ? nullptr : parsed.data.data()) == 0;
        if (!mounted[nosuid]) {
            setup = false;
            break;
        }
        for (unsigned bits = 1; bits <= 3; ++bits) {
            if (remote)
                snprintf(images[nosuid][bits - 1], sizeof(images[nosuid][bits - 1]), "%s/%s-image%u", paths[nosuid], argv[3], bits);
            else
                snprintf(images[nosuid][bits - 1], sizeof(images[nosuid][bits - 1]), "%s/goal19-nosuid-%ld-image%u", paths[nosuid],
                         static_cast<long>(getpid()), bits);
            // The runner prepares remote modes on the owner: mapped chmod is
            // not implemented. Both client mounts use those same three files.
            if (!remote && !copy_image(argv[0], images[nosuid][bits - 1], bits)) {
                setup = false;
                break;
            }
        }
    }
    if (!setup) printf("setup failure errno=%d\n", errno);
    for (unsigned nosuid = 0; setup && nosuid < 2; ++nosuid) {
        struct statvfs path_stat{}, fd_stat{};
        int fd = open(paths[nosuid], O_RDONLY | O_DIRECTORY);
        bool flags = fd >= 0 && statvfs(paths[nosuid], &path_stat) == 0 && fstatvfs(fd, &fd_stat) == 0 &&
                     !!(path_stat.f_flag & ST_NOSUID) == nosuid && !!(fd_stat.f_flag & ST_NOSUID) == nosuid;
        bool visible = mount_options_visible(paths[nosuid], nosuid, remote);
        flags &= visible;
        if (fd >= 0) close(fd);
        printf("%s mount=%u statvfs=%lu fstatvfs=%lu proc_mounts=%d\n", flags ? "PASS" : "FAIL", nosuid, path_stat.f_flag, fd_stat.f_flag,
               visible);
        failures += !flags;
        for (unsigned nnp = 0; nnp < 2; ++nnp)
            for (unsigned bits = 1; bits <= 3; ++bits)
                for (unsigned method = 0; method < 6; ++method) {
                    pid_t child = fork();
                    if (child == 0) _exit(run_child(images[nosuid][bits - 1], nosuid, nnp, bits, method));
                    int status = wait_child(child);
                    printf("%s nosuid=%u nnp=%u bits=%u method=%u status=%d\n", status == 0 ? "PASS" : "FAIL", nosuid, nnp, bits, method,
                           status);
                    failures += status != 0;
                    ++cases;
                }
    }
    bool cleanup = true;
    if (remote && mounted[0]) {
        for (unsigned bits = 0; bits < 3; ++bits) {
            if (images[0][bits][0] && unlink(images[0][bits]) && errno != ENOENT) cleanup = false;
        }
    }
    for (unsigned i = 0; i < 2; ++i) {
        if (mounted[i] && umount(paths[i])) cleanup = false;
        if (made[i] && rmdir(paths[i])) cleanup = false;
    }
    if (rmdir(argv[1])) cleanup = false;
    printf("mount fixture cleanup %s\n", cleanup ? "completed" : "FAILED");
    bool ok = setup && cleanup && failures == 0 && cases == 72;
    printf("%s: %u native nosuid exec cases\n", ok ? "PASS" : "FAIL", cases);
    return ok ? 0 : 1;
}
