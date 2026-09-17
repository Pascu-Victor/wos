// Native syscall regression for unsupported synthetic filesystem mutations.
#include <errno.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {
auto check(const char* path, int expected) -> bool {
    errno = 0;
    int const RESULT = mkdir(path, 0777);
    int const ERROR = errno;
    printf("uid=%u mkdir %s: result=%d errno=%d\n", static_cast<unsigned>(getuid()), path, RESULT, ERROR);
    if (RESULT != -1 || ERROR != expected) return false;
    if (expected == ENOSYS) {
        struct stat missing{};
        return lstat(path, &missing) == -1 && errno == ENOENT;
    }
    return true;
}
}  // namespace

auto main() -> int {
    unsigned checks = 0;
    const char* const existing[] = {"/dev", "/dev/pts", "/dev/null", "/proc", "/proc/version", "/proc/self", "/proc/self/"};
    const char* const absent[] = {"/dev/goal19_mkdir_absent", "/proc/goal19_mkdir_absent"};
    for (unsigned profile = 0; profile < 2; ++profile) {
        if (profile != 0 && (setgid(2000) != 0 || setuid(1000) != 0)) return 2;
        for (const char* path : existing) {
            if (!check(path, EEXIST)) return 1;
            ++checks;
        }
        for (const char* path : absent) {
            if (!check(path, ENOSYS)) return 1;
            ++checks;
        }
        if (!check("/dev/goal19_mkdir_absent/child", ENOENT) || !check("/dev/null/child", ENOTDIR) ||
            !check("/proc/version/child", ENOTDIR))
            return 1;
        checks += 3;
    }
    printf("PASS: %u synthetic mkdir denials with absent paths preserved\n", checks);
    return 0;
}
