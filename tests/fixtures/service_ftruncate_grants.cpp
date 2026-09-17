// Functional admitted-descriptor truncate checks for local and WKI filesystems.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <initializer_list>
namespace {
bool equal(timespec a, timespec b) { return a.tv_sec == b.tv_sec && a.tv_nsec == b.tv_nsec; }
bool unchanged(const struct stat& a, const struct stat& b) {
    return a.st_ino == b.st_ino && a.st_size == b.st_size && a.st_mode == b.st_mode && a.st_nlink == b.st_nlink && a.st_uid == b.st_uid &&
           a.st_gid == b.st_gid && equal(a.st_mtim, b.st_mtim) && equal(a.st_ctim, b.st_ctim);
}
bool create(const char* path, const char* text) {
    int fd = open(path, O_CREAT | O_EXCL | O_RDWR, 0666);
    bool ok = fd >= 0 && write(fd, text, 6) == 6;
    if (fd >= 0) close(fd);
    return ok;
}
bool wait(pid_t child) {
    int status = 0;
    return child > 0 && waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}
}  // namespace
int main(int argc, char** argv) {
    if (argc != 2) return 2;
    setvbuf(stdout, nullptr, _IOLBF, 0);
    umask(0);
    unsigned cases = 0, failed = 0;
    for (unsigned uid : {0U, 1001U})
        for (int mode : {O_RDONLY, O_WRONLY, O_RDWR})
            for (unsigned history = 0; history < 4; ++history)
                for (off_t length : {-1L, 0L, 3L, 6L, 9L}) {
                    char path[512], moved[512];
                    snprintf(path, sizeof(path), "%s/file%u", argv[1], cases);
                    snprintf(moved, sizeof(moved), "%s/moved%u", argv[1], cases);
                    if (!create(path, "abcdef")) return 90;
                    int subject = open(path, mode), observer = open(path, O_RDONLY);
                    if (subject < 0 || observer < 0 || lseek(subject, 2, SEEK_SET) != 2) return 91;
                    if (history == 1 && chmod(path, 0)) return 92;
                    if (history == 2 && (rename(path, moved) || !create(path, "ghijkl"))) return 93;
                    if (history == 3 && unlink(path)) return 94;
                    struct stat before{}, after{}, decoy{};
                    if (fstat(observer, &before) || (history == 2 && stat(path, &decoy))) return 95;
                    int expected = length < 0 ? EINVAL : mode == O_RDONLY ? EBADF : 0;
                    pid_t child = fork();
                    if (child == 0) {
                        if (setgroups(0, nullptr) || setgid(2001) || setuid(uid)) _exit(96);
                        errno = 0;
                        int r = ftruncate(subject, length), e = errno;
                        bool ok = expected ? r == -1 && e == expected : r == 0;
                        if (!ok && failed < 12)
                            printf("operation uid=%u mode=%d history=%u size=%ld result=%d errno=%d expected=%d\n", uid, mode, history,
                                   long(length), r, e, expected);
                        _exit(ok ? 0 : 1);
                    }
                    bool ok = wait(child) && fstat(observer, &after) == 0 && lseek(subject, 0, SEEK_CUR) == 2;
                    if (expected)
                        ok = unchanged(before, after) && ok;
                    else
                        ok = after.st_size == length && after.st_ino == before.st_ino && after.st_mode == before.st_mode &&
                             after.st_uid == before.st_uid && after.st_gid == before.st_gid && ok;
                    char bytes[9]{};
                    ssize_t got = pread(observer, bytes, sizeof(bytes), 0);
                    size_t size = expected ? 6 : length;
                    ok = got == ssize_t(size) && memcmp(bytes, "abcdef", size < 6 ? size : 6) == 0 && ok;
                    if (size > 6) ok = bytes[6] == 0 && bytes[7] == 0 && bytes[8] == 0 && ok;
                    if (history == 2) {
                        struct stat now{};
                        int fd = open(path, O_RDONLY);
                        char text[6]{};
                        ok = fd >= 0 && read(fd, text, 6) == 6 && memcmp(text, "ghijkl", 6) == 0 && stat(path, &now) == 0 &&
                             unchanged(decoy, now) && ok;
                        if (fd >= 0) close(fd);
                    }
                    close(subject);
                    close(observer);
                    if (history != 3 && unlink(path)) return 97;
                    if (history == 2 && unlink(moved)) return 98;
                    if (!ok) {
                        if (failed < 12)
                            printf("FAIL case=%u uid=%u mode=%d history=%u length=%ld\n", cases, uid, mode, history, long(length));
                        ++failed;
                    }
                    ++cases;
                }
    // O_RDONLY|O_TRUNC is a separately authorized open, followed by a read-only FD.
    for (unsigned uid : {0U, 1001U}) {
        char path[512];
        snprintf(path, sizeof(path), "%s/open%u", argv[1], uid);
        if (!create(path, "abcdef")) return 99;
        pid_t child = fork();
        if (child == 0) {
            if (setgroups(0, nullptr) || setgid(2001) || setuid(uid)) _exit(100);
            int fd = open(path, O_RDONLY | O_TRUNC);
            struct stat s{};
            bool ok = fd >= 0 && fstat(fd, &s) == 0 && s.st_size == 0;
            errno = 0;
            int r = fd < 0 ? -1 : ftruncate(fd, 3);
            ok = r == -1 && errno == EBADF && ok;
            if (fd >= 0) close(fd);
            _exit(ok ? 0 : 1);
        }
        struct stat s{};
        bool ok = wait(child) && stat(path, &s) == 0 && s.st_size == 0;
        if (unlink(path)) return 101;
        if (!ok) {
            printf("FAIL truncating-open uid=%u\n", uid);
            ++failed;
        }
        ++cases;
    }
    int directory = open(argv[1], O_RDONLY | O_DIRECTORY);
    if (directory < 0) return 102;
    for (off_t length : {-1L, 0L}) {
        errno = 0;
        bool ok = ftruncate(directory, length) == -1 && errno == EISDIR;
        if (!ok) ++failed;
        ++cases;
    }
    close(directory);
    errno = 0;
    if (ftruncate(-1, 0) != -1 || errno != EBADF) ++failed;
    ++cases;
    int pipes[2];
    if (pipe(pipes)) return 103;
    errno = 0;
    if (ftruncate(pipes[1], 0) != -1 || errno != ENOSYS) ++failed;
    close(pipes[0]);
    close(pipes[1]);
    ++cases;
    printf("%s truncate checks=%u failed=%u\n", failed ? "FAIL" : "PASS", cases, failed);
    return failed ? 1 : 0;
}
