// Exercise ordinary remote OPEN from local, irreversibly restricted callers.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {
auto control(ker::abi::process::CredentialControl op, unsigned value) -> long long {
    return ker::process::prctl(static_cast<int>(op), value, 0, 0, 0);
}
auto restrict_caller(const char* profile) -> bool {
    const bool root = strcmp(profile, "root") == 0;
    const bool service = strcmp(profile, "service") == 0;
    const bool group = strcmp(profile, "group") == 0;
    const bool other = strcmp(profile, "other") == 0;
    if (!root && !service && !group && !other && strcmp(profile, "inert") != 0) return false;
    gid_t groups[]{3000, 3001, 3002, 3003, 3004, 3005, 3006, group ? 2000U : 3007U};
    using Control = ker::abi::process::CredentialControl;
    if (setgroups(8, groups) || ker::process::prctl(38, 1, 0, 0, 0) || control(Control::SET_SERVICE_PRIVILEGES, root ? 0 : 3)) return false;
    if (!root && (setgid(group || other ? 2001 : 2000) || setuid(group || other ? 1001 : 1000))) return false;
    return control(Control::DROP_AUTHORITY_CEILING, service ? ker::abi::process::service_privilege_ceiling(3) : 0) == 0;
}
}  // namespace

auto main(int argc, char** argv) -> int {
    if (argc != 5 || geteuid() != 0) return 2;
    const char* operation = argv[2];
    const char* path = argv[3];
    const int expected = atoi(argv[4]);
    int flags = O_RDONLY;
    bool create = false;
    bool truncate = false;
    if (strcmp(operation, "create") == 0) {
        flags = O_RDWR | O_CREAT | O_EXCL;
        create = true;
    } else if (strcmp(operation, "truncate") == 0) {
        flags = O_WRONLY | O_TRUNC;
        truncate = true;
    } else if (strcmp(operation, "write") == 0)
        flags = O_WRONLY;
    else if (strcmp(operation, "directory") == 0)
        flags = O_RDONLY | O_DIRECTORY;
    else if (strcmp(operation, "nofollow") == 0)
        flags = O_RDONLY | O_NOFOLLOW;
    else if (strcmp(operation, "read") != 0 && strcmp(operation, "preserved") != 0 && strcmp(operation, "empty") != 0)
        return 2;
    umask(0);
    if (!restrict_caller(argv[1])) return 2;
    errno = 0;
    int fd = open(path, flags, 0640);
    int error = fd < 0 ? errno : 0;
    if (error != expected) {
        printf("FAIL profile=%s operation=%s errno=%d expected=%d\n", argv[1], operation, error, expected);
        if (fd >= 0) close(fd);
        return 1;
    }
    if (fd >= 0) {
        struct stat st{};
        bool valid = fstat(fd, &st) == 0;
        if (create) {
            valid &= st.st_uid == geteuid() && st.st_gid == getegid() && (st.st_mode & 0777) == 0640;
            valid &= write(fd, "preserve", 8) == 8;
        }
        if (truncate || strcmp(operation, "empty") == 0) valid &= st.st_size == 0;
        if (strcmp(operation, "preserved") == 0) {
            char bytes[9]{};
            valid &= read(fd, bytes, sizeof(bytes)) == 8 && memcmp(bytes, "preserve", 8) == 0;
        }
        if (strcmp(operation, "directory") == 0) valid &= S_ISDIR(st.st_mode);
        valid &= close(fd) == 0;
        if (!valid) {
            puts("FAIL: open metadata/data/close invariant");
            return 1;
        }
    }
    printf("PASS profile=%s operation=%s errno=%d\n", argv[1], operation, error);
    return 0;
}
