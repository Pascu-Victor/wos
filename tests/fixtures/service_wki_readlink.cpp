// Read remote symlink text as an irreversibly restricted caller, preserving error output.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <sys/vfs.h>
#include <unistd.h>

namespace {
auto restrict_caller(const char* profile) -> bool {
    if (strcmp(profile, "fullroot") == 0) return true;
    bool const ROOT = strcmp(profile, "root") == 0;
    bool const GROUP = strcmp(profile, "group") == 0;
    bool const OTHER = strcmp(profile, "other") == 0;
    bool const SERVICE = strcmp(profile, "service") == 0;
    if (!ROOT && !GROUP && !OTHER && !SERVICE && strcmp(profile, "inert") != 0) return false;
    gid_t groups[]{3000, 3001, 3002, 3003, 3004, 3005, 3006, GROUP ? 2000U : 3007U};
    using Control = ker::abi::process::CredentialControl;
    if (setgroups(8, groups) || ker::process::prctl(static_cast<int>(Control::SET_NO_NEW_PRIVILEGES), 1, 0, 0, 0) ||
        ker::process::prctl(static_cast<int>(Control::SET_SERVICE_PRIVILEGES), ROOT ? 0 : 3, 0, 0, 0))
        return false;
    if (!ROOT && (setgid(GROUP || OTHER ? 2001 : 2000) || setuid(GROUP || OTHER ? 1001 : 1000))) return false;
    return ker::process::prctl(static_cast<int>(Control::DROP_AUTHORITY_CEILING),
                               SERVICE ? ker::abi::process::service_privilege_ceiling(3) : 0, 0, 0, 0) == 0;
}
}  // namespace

auto main(int argc, char** argv) -> int {
    if (argc == 5 && strcmp(argv[1], "--owner-mkdir") == 0) {
        utsname node{};
        struct stat st{};
        umask(0);
        bool const VALID = geteuid() == 1000 && getegid() == 2000 && uname(&node) == 0 && strcmp(node.nodename, argv[4]) == 0 &&
                           mkdir(argv[2], strtoul(argv[3], nullptr, 8)) == 0 && stat(argv[2], &st) == 0 && st.st_uid == 1000 &&
                           st.st_gid == 2000 && S_ISDIR(st.st_mode);
        printf("%s owner-local directory uid=%u gid=%u errno=%d\n", VALID ? "PASS" : "FAIL", geteuid(), getegid(), errno);
        return VALID ? 0 : 1;
    }
    if (argc == 5 && strcmp(argv[1], "--stage-owner") == 0 && geteuid() == 0) {
        utsname source{};
        char executable[512]{};
        if (uname(&source) != 0 || ker::process::setwkitarget(argv[2], strlen(argv[2]), ker::process::WKI_TARGET_FLAG_STRICT) != 0 ||
            ker::abi::vfs::wki_rule_add_vfs("/tmp", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL) != 0 || !restrict_caller("inert"))
            return 2;
        if (snprintf(executable, sizeof(executable), "/wki/%s%s", source.nodename, argv[0]) >= static_cast<int>(sizeof(executable)))
            return 2;
        char operation[] = "--owner-mkdir";
        char* arguments[]{executable, operation, argv[3], argv[4], argv[2], nullptr};
        execv(executable, arguments);
        printf("FAIL owner-local staging exec errno=%d\n", errno);
        return 1;
    }
    bool const LOCAL_DEVFS_ROUTE = argc == 8 && strcmp(argv[7], "--local-devfs-route") == 0;
    if ((argc != 7 && !LOCAL_DEVFS_ROUTE) || geteuid() != 0) return 2;
    if (LOCAL_DEVFS_ROUTE && ker::abi::vfs::wki_rule_add_vfs("/dev", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL) != 0) return 2;
    if (!restrict_caller(argv[1])) return 2;
    size_t const SIZE = strtoul(argv[6], nullptr, 10);
    if (SIZE > 1024) return 2;
    char output[1024];
    memset(output, 'Z', sizeof(output));
    int const EXPECTED = atoi(argv[4]);
    ssize_t result = -1;
    errno = 0;
    if (strcmp(argv[2], "create") == 0) {
        int const FD = open(argv[3], O_CREAT | O_EXCL | O_WRONLY, 0000);
        if (FD >= 0) result = close(FD);
    } else if (strcmp(argv[2], "raw") == 0) {
        result = ker::abi::vfs::readlink(argv[3], output, SIZE);
        if (result < 0) {
            errno = -result;
            result = -1;
        }
    } else if (strcmp(argv[2], "at") == 0) {
        char parent[512]{};
        if (strlen(argv[3]) >= sizeof(parent)) return 2;
        strcpy(parent, argv[3]);
        char* final = strrchr(parent, '/');
        if (!final || final == parent || final[1] == 0) return 2;
        *final++ = 0;
        int const FD = open(parent, O_RDONLY | O_DIRECTORY);
        if (FD >= 0) {
            result = readlinkat(FD, final, output, SIZE);
            int const SAVED_ERROR = errno;
            if (close(FD) != 0) return 2;
            errno = SAVED_ERROR;
        }
    } else if (strcmp(argv[2], "readlink") == 0) {
        result = readlink(argv[3], output, SIZE);
    } else
        return 2;
    int const ERROR = result < 0 ? errno : 0;
    size_t const TARGET = strlen(argv[5]);
    size_t const COPIED = TARGET < SIZE ? TARGET : SIZE;
    bool valid = ERROR == EXPECTED;
    size_t unchanged = 0;
    if (ERROR == 0) {
        valid &= result >= 0 && static_cast<size_t>(result) == COPIED && memcmp(output, argv[5], COPIED) == 0;
        unchanged = result >= 0 && static_cast<size_t>(result) <= sizeof(output) ? static_cast<size_t>(result) : 0;
    }
    for (size_t i = unchanged; i < sizeof(output); ++i) valid &= output[i] == 'Z';
    printf("%s profile=%s operation=%s errno=%d expected=%d bytes=%zd size=%zu tail-checked\n", valid ? "PASS" : "FAIL", argv[1], argv[2],
           ERROR, EXPECTED, result, SIZE);
    return valid ? 0 : 1;
}
