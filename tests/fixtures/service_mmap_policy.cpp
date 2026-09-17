// Ordinary mmap/mprotect/msync regression for admitted open descriptions.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <initializer_list>

namespace {
constexpr size_t PAGE = 4096;

bool seed(const char* path, unsigned char value) {
    int fd = open(path, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd < 0) return false;
    unsigned char bytes[PAGE];
    memset(bytes, value, sizeof(bytes));
    bool ok = write(fd, bytes, sizeof(bytes)) == static_cast<ssize_t>(sizeof(bytes)) && fsync(fd) == 0;
    return close(fd) == 0 && ok;
}

bool first_byte(const char* path, unsigned char expected) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) return false;
    unsigned char byte{};
    bool ok = pread(fd, &byte, 1, 0) == 1 && byte == expected;
    return close(fd) == 0 && ok;
}

bool run_case(const char* base, unsigned index, int mode, bool shared, int prot, unsigned profile) {
    char original[512], replacement[512];
    snprintf(original, sizeof(original), "%s/%u-%u-original", base, profile, index);
    snprintf(replacement, sizeof(replacement), "%s/%u-%u-replacement", base, profile, index);
    bool ok = seed(original, 0x41) && seed(replacement, 0x42);
    int fd = ok ? open(original, mode) : -1;
    auto* area = static_cast<unsigned char*>(mmap(nullptr, 2 * PAGE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    ok = ok && fd >= 0 && area != MAP_FAILED;
    int map_error = 0, protect_error = 0;
    bool const DENIED = mode == O_WRONLY || (shared && (prot & PROT_WRITE) && mode == O_RDONLY);
    bool wrote_shared = false;
    if (ok) {
        area[0] = 0x5a;
        area[PAGE] = 0x6b;
        ok = lseek(fd, 7, SEEK_SET) == 7;
        errno = 0;
        void* mapped = mmap(area, PAGE, prot, MAP_FIXED | (shared ? MAP_SHARED : MAP_PRIVATE), fd, 0);
        map_error = errno;
        if (DENIED) {
            ok = ok && mapped == MAP_FAILED && map_error == EACCES && area[0] == 0x5a && area[PAGE] == 0x6b;
            if (mapped == MAP_FAILED) {
                area[0] = 0x5c;
                ok = ok && area[0] == 0x5c;
            }
        } else {
            ok = ok && mapped == area && area[PAGE] == 0x6b && lseek(fd, 0, SEEK_CUR) == 7;
            // Reuse the descriptor before faulting PROT_NONE mappings or
            // requesting a later write upgrade. The mapping must keep its File.
            int const OLD_FD = fd;
            ok = close(fd) == 0 && ok;
            fd = open(replacement, O_RDWR);
            ok = ok && fd == OLD_FD;
            errno = 0;
            int protected_result = mprotect(area, PAGE, PROT_READ | PROT_WRITE);
            protect_error = errno;
            if (shared && mode == O_RDONLY) {
                ok = ok && protected_result == -1 && protect_error == EACCES;
                ok = mprotect(area, PAGE, PROT_READ) == 0 && ok;
                if (ok) ok = area[0] == 0x41 && msync(area, PAGE, MS_SYNC) == 0;
            } else {
                ok = ok && protected_result == 0;
                if (ok) {
                    ok = area[0] == 0x41;
                    area[0] = 0x51;
                    ok = msync(area, PAGE, MS_SYNC) == 0 && ok;
                    wrote_shared = shared;
                }
            }
        }
    }
    if (fd >= 0) ok = close(fd) == 0 && ok;
    if (area != MAP_FAILED) ok = munmap(area, 2 * PAGE) == 0 && ok;
    ok = first_byte(original, wrote_shared ? 0x51 : 0x41) && first_byte(replacement, 0x42) && ok;
    bool const ORIGINAL_REMOVED = unlink(original) == 0;
    bool const REPLACEMENT_REMOVED = unlink(replacement) == 0;
    bool const CLEAN = ORIGINAL_REMOVED && REPLACEMENT_REMOVED;
    ok = CLEAN && ok;
    printf("%s profile=%u case=%u mode=%d shared=%d prot=%d mmap_errno=%d mprotect_errno=%d cleanup=%d\n", ok ? "PASS" : "FAIL", profile,
           index, mode, shared, prot, map_error, protect_error, CLEAN);
    return ok;
}

int run_profile(const char* base, unsigned profile) {
    // WOS implements setgid/setuid; a privileged permanent drop updates all
    // three IDs. Verify saved IDs too before accepting the non-root profile.
    if (profile != 0 && (setgroups(0, nullptr) || setgid(100) || setuid(100))) {
        printf("FAIL profile setup=%u errno=%d\n", profile, errno);
        return 90;
    }
    uid_t ruid{}, euid{}, suid{};
    gid_t rgid{}, egid{}, sgid{};
    unsigned const ID = profile == 0 ? 0 : 100;
    bool ok = getresuid(&ruid, &euid, &suid) == 0 && getresgid(&rgid, &egid, &sgid) == 0 && ruid == ID && euid == ID && suid == ID &&
              rgid == ID && egid == ID && sgid == ID;
    if (!ok) {
        printf("FAIL profile identity=%u uid=%u:%u:%u gid=%u:%u:%u errno=%d\n", profile, ruid, euid, suid, rgid, egid, sgid, errno);
        return 91;
    }
    unsigned index = 0, failed = 0;
    for (int mode : {O_RDONLY, O_WRONLY, O_RDWR}) {
        for (bool shared : {false, true}) {
            for (int prot : {PROT_NONE, PROT_READ, PROT_READ | PROT_WRITE}) {
                if (!run_case(base, index++, mode, shared, prot, profile)) ++failed;
            }
        }
    }
    return failed ? 1 : 0;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc != 2 || getuid() != 0 || strlen(argv[1]) > 400) return 2;
    setvbuf(stdout, nullptr, _IOLBF, 0);
    if (mkdir(argv[1], 0700) || chmod(argv[1], 0777)) return 3;
    unsigned failed = 0;
    for (unsigned profile : {0U, 1U}) {
        pid_t child = fork();
        if (child == 0) _exit(run_profile(argv[1], profile));
        int status{};
        if (child < 0 || waitpid(child, &status, 0) != child || !WIFEXITED(status) || WEXITSTATUS(status)) ++failed;
    }
    bool const CLEAN = rmdir(argv[1]) == 0;
    printf("%s mmap policy checks=36 failed_profiles=%u cleanup=%d\n", failed == 0 && CLEAN ? "PASS" : "FAIL", failed, CLEAN);
    return failed == 0 && CLEAN ? 0 : 1;
}
