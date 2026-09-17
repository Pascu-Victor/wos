// Ordinary exec must preserve the retained cwd's client mount restrictions.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <initializer_list>

namespace {
bool seed(const char* path) {
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_NO_CACHE, 0666);
    if (fd < 0) return false;
    bool ok = write(fd, "seed", 4) == 4 && fsync(fd) == 0 && fchmod(fd, 0666) == 0;
    return close(fd) == 0 && ok;
}
bool contents(const char* path, const char* expected) {
    char bytes[4]{};
    int fd = open(path, O_RDONLY | O_NO_CACHE);
    if (fd < 0) return false;
    bool ok = read(fd, bytes, sizeof(bytes)) == 4 && !memcmp(bytes, expected, 4);
    return close(fd) == 0 && ok;
}
struct Execution {
    char* image;
    const char* peer;
    bool readonly;
    bool remote;
    unsigned profile;
    unsigned long long inode;
};
void* execute(void* opaque) {
    auto& request = *static_cast<Execution*>(opaque);
    errno = 0;
    int fd = open("file", O_WRONLY | O_NO_CACHE);
    int error = errno;
    bool baseline = request.readonly ? fd < 0 && error == EROFS : fd >= 0;
    if (fd >= 0) baseline &= close(fd) == 0;
    printf("%s before-exec readonly=%d profile=%u remote=%d open=%d errno=%d\n", baseline ? "PASS" : "FAIL", request.readonly,
           request.profile, request.remote, fd, error);
    if (!baseline) _exit(3);
    auto result = request.remote ? ker::process::setwkitarget(request.peer, strlen(request.peer), ker::process::WKI_TARGET_FLAG_STRICT)
                                 : ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL);
    if (result) _exit(4);
    char mode[] = "--payload", readonly[8], profile[16], inode[32];
    snprintf(readonly, sizeof(readonly), "%d", request.readonly);
    snprintf(profile, sizeof(profile), "%u", request.profile);
    snprintf(inode, sizeof(inode), "%llu", request.inode);
    char* args[] = {request.image, mode, readonly, profile, inode, nullptr};
    execv(request.image, args);
    printf("FAIL exec errno=%d\n", errno);
    _exit(5);
}
int payload(bool readonly, unsigned profile, unsigned long long inode) {
    uid_t r, e, s;
    gid_t gr, ge, gs;
    unsigned expected = profile ? 100 : 0;
    bool identity = !getresuid(&r, &e, &s) && !getresgid(&gr, &ge, &gs) && r == expected && e == expected && s == expected &&
                    gr == expected && ge == expected && gs == expected && getgroups(0, nullptr) == 0;
    struct stat st{};
    bool object = !stat("file", &st) && static_cast<unsigned long long>(st.st_ino) == inode && contents("file", "seed");
    errno = 0;
    int fd = open("file", O_WRONLY | O_NO_CACHE);
    int error = errno;
    bool wrote = false;
    if (fd >= 0) {
        wrote = write(fd, "edit", 4) == 4 && fsync(fd) == 0;
        wrote &= close(fd) == 0;
    }
    bool policy = readonly ? fd < 0 && error == EROFS && contents("file", "seed") : fd >= 0 && wrote && contents("file", "edit");
    char cwd[512]{};
    printf("%s payload identity=%d object=%d readonly=%d open=%d errno=%d wrote=%d cwd=%s\n",
           identity && object && policy ? "PASS" : "FAIL", identity, object, readonly, fd, error, wrote,
           getcwd(cwd, sizeof(cwd)) ? cwd : "<error>");
    return identity && object && policy ? 0 : 1;
}
bool run(Execution request, bool worker, const char* owner_file) {
    if (!seed(owner_file)) return false;
    pid_t child = fork();
    if (child == 0) {
        if (setgroups(0, nullptr) || (request.profile && (setgid(100) || setuid(100)))) _exit(6);
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
    bool data = contents(owner_file, request.readonly ? "seed" : "edit");
    bool ok = child > 0 && reaped == child && WIFEXITED(status) && WEXITSTATUS(status) == 0 && data;
    printf("%s case readonly=%d profile=%u worker=%d remote=%d status=%d owner_data=%d\n", ok ? "PASS" : "FAIL", request.readonly,
           request.profile, worker, request.remote, status, data);
    return ok;
}
}  // namespace

int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc == 5 && !strcmp(argv[1], "--payload"))
        return payload(atoi(argv[2]) != 0, static_cast<unsigned>(atoi(argv[3])), strtoull(argv[4], nullptr, 10));
    if (argc != 4 || geteuid() || (strcmp(argv[2], "/tmp") && strcmp(argv[2], "/")) || strlen(argv[1]) > 64 || strlen(argv[3]) > 64)
        return 2;
    if (ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL) ||
        ker::abi::vfs::wki_rule_add_vfs("/", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL))
        return 2;
    printf("BEGIN cwd mount policy pid=%d export=%s suffix=%s\n", getpid(), argv[2], argv[3]);
    char peer_root[256], source[256], writable[256];
    snprintf(peer_root, sizeof(peer_root), "/wki/%s", argv[1]);
    snprintf(source, sizeof(source), "wki://%s//", argv[1]);
    snprintf(writable, sizeof(writable), "/tmp/goal19-cwd-policy-%s-rw", argv[3]);
    struct SavedMount {
        std::array<char, 256> path{};
        unsigned long flags{};
        bool detached{};
    };
    std::array<SavedMount, 16> saved{};
    size_t count = 0;
    FILE* mounts = fopen("/proc/mounts", "r");
    if (!mounts) {
        printf("FAIL mounts open errno=%d\n", errno);
        return 2;
    }
    char line[1024], kind[32], path[256], type[32], flags[64];
    bool parsed = true;
    while (fgets(line, sizeof(line), mounts)) {
        printf("MOUNT_ROW %s", line);
        if (sscanf(line, "%31s %255s %31s %63s", kind, path, type, flags) != 4) {
            parsed = false;
            break;
        }
        size_t prefix = strlen(peer_root);
        if (strcmp(kind, "remote") || strncmp(path, peer_root, prefix) || (path[prefix] && path[prefix] != '/')) continue;
        if (count == saved.size() ||
            (strcmp(flags, "rw") && strcmp(flags, "ro") && strcmp(flags, "rw,nosuid") && strcmp(flags, "ro,nosuid"))) {
            parsed = false;
            break;
        }
        strcpy(saved[count].path.data(), path);
        saved[count].flags = (flags[1] == 'o' ? MS_RDONLY : 0) | (strstr(flags, "nosuid") ? MS_NOSUID : 0);
        ++count;
    }
    parsed &= !ferror(mounts);
    fclose(mounts);
    if (!parsed || !count) {
        printf("FAIL mounts parsed=%d count=%lu errno=%d\n", parsed, count, errno);
        return 2;
    }
    std::sort(saved.begin(), saved.begin() + count,
              [](const SavedMount& a, const SavedMount& b) { return strlen(a.path.data()) > strlen(b.path.data()); });
    auto unmount = [](const char* target) {
        for (unsigned attempt = 0; attempt < 50; ++attempt) {
            if (!umount(target)) return true;
            if (errno != EBUSY) break;
            usleep(100000);
        }
        printf("FAIL unmount path=%s errno=%d\n", target, errno);
        return false;
    };
    int wki_directory = open("/wki", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    bool writable_mounted = false;
    bool setup = wki_directory >= 0 && mkdir(writable, 0755) == 0;
    if (setup) {
        writable_mounted = mount(source, writable, "remote", 0, nullptr) == 0;
        setup &= writable_mounted;
    }
    for (size_t i = 0; setup && i < count; ++i) {
        saved[i].detached = unmount(saved[i].path.data());
        setup &= saved[i].detached;
    }
    // Keep native mount targets available without pathname lookup auto-mounting
    // the peer root. Relative operations use the exact native parent directory.
    if (setup && mkdirat(wki_directory, argv[1], 0755) && errno != EEXIST) setup = false;
    int peer_directory = setup ? openat(wki_directory, argv[1], O_RDONLY | O_DIRECTORY | O_CLOEXEC) : -1;
    if (setup && peer_directory < 0) setup = false;
    for (size_t i = 0; setup && i < count; ++i) {
        const char* suffix = saved[i].path.data() + strlen(peer_root);
        if (!*suffix) continue;
        ++suffix;
        if (strchr(suffix, '/') || (mkdirat(peer_directory, suffix, 0755) && errno != EEXIST)) setup = false;
    }
    if (peer_directory >= 0) close(peer_directory);
    if (wki_directory >= 0) close(wki_directory);
    unsigned checks = 0, failed = !setup;
    bool cleanup = true;
    for (bool readonly : {false, true}) {
        if (!setup) break;
        char base[128], owner[512], owner_file[512], cwd[512];
        snprintf(base, sizeof(base), "goal19-cwd-policy-%s-%d", argv[3], readonly);
        snprintf(owner, sizeof(owner), "%s%s%s%s", writable, argv[2], strcmp(argv[2], "/") ? "/" : "", base);
        snprintf(owner_file, sizeof(owner_file), "%s/file", owner);
        snprintf(cwd, sizeof(cwd), "%s%s%s%s", peer_root, argv[2], strcmp(argv[2], "/") ? "/" : "", base);
        bool mounted = false, made_owner = false;
        auto prepare = [&] {
            if (mkdir(owner, 0755)) return false;
            made_owner = true;
            if (!seed(owner_file)) return false;
            if (mount(source, peer_root, "remote", readonly ? MS_RDONLY | MS_NOSUID : 0, nullptr)) return false;
            mounted = true;
            return chdir(cwd) == 0;
        };
        struct stat st{};
        if (prepare() && !stat(owner_file, &st)) {
            for (unsigned profile : {0U, 1U})
                for (bool worker : {false, true})
                    for (bool remote : {false, true}) {
                        ++checks;
                        failed += !run({argv[0], argv[1], readonly, remote, profile, static_cast<unsigned long long>(st.st_ino)}, worker,
                                       owner_file);
                    }
        } else {
            printf("FAIL setup readonly=%d errno=%d\n", readonly, errno);
            ++failed;
        }
        printf("CLEANUP readonly=%d leaving cwd\n", readonly);
        cleanup &= chdir("/") == 0;
        if (mounted) cleanup &= unmount(peer_root);
        if (made_owner) {
            int result = unlink(owner_file);
            cleanup &= result == 0 || errno == ENOENT;
            cleanup &= rmdir(owner) == 0;
        }
    }
    // Restore child mounts while their parent is native, then the peer root.
    // Public mount admission does not walk through a REMOTE parent.
    for (size_t i = 0; i < count; ++i) {
        if (!saved[i].detached) continue;
        auto* target = saved[i].path.data();
        const char* export_name = target + strlen(peer_root);
        if (!*export_name) export_name = "/";
        char original_source[512];
        snprintf(original_source, sizeof(original_source), "wki://%s/%s", argv[1], export_name);
        int result = mount(original_source, target, "remote", saved[i].flags, nullptr);
        printf("RESTORE path=%s flags=%lu result=%d errno=%d\n", target, saved[i].flags, result, errno);
        cleanup &= result == 0;
    }
    if (writable_mounted) cleanup &= unmount(writable);
    if (rmdir(writable) && errno != ENOENT) cleanup = false;
    printf("%s cwd mount policy checks=%u failed=%u cleanup=%d\n", failed || !cleanup ? "FAIL" : "PASS", checks, failed, cleanup);
    return failed || !cleanup;
}
