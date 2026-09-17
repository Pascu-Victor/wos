// Run locally and send metadata requests as an irreversibly restricted caller.
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
    if (argc < 5 || geteuid() != 0 || !restrict_caller(argv[1])) return 2;
    const char* operation = argv[2];
    int const EXPECTED = atoi(argv[4]);
    struct stat output;
    memset(&output, 0x5a, sizeof(output));
    struct stat const BEFORE = output;
    bool valid = true;
    int result = -1;
    errno = 0;
    if (strcmp(operation, "batch") == 0) {
        if (argc < 7 || (argc - 5) % 2 != 0 || (argc - 5) / 2 > 64) return 2;
        size_t const COUNT = (argc - 5) / 2;
        ker::abi::vfs::metadata_batch_entry entries[64]{};
        ker::abi::vfs::metadata_batch_result results[64]{};
        for (size_t i = 0; i < COUNT; ++i) entries[i].path = argv[5 + i * 2];
        ker::abi::vfs::metadata_batch_header header{};
        header.version = ker::abi::vfs::METADATA_BATCH_VERSION;
        header.operation = ker::abi::vfs::metadata_batch_operation::STAT_FOLLOW;
        header.count = COUNT;
        result = ker::abi::vfs::metadata_batch(&header, entries, results);
        valid = result == -EXPECTED;
        struct stat const ZERO{};
        for (size_t i = 0; i < COUNT; ++i) {
            int const ITEM = result == 0 ? -atoi(argv[6 + i * 2]) : -EINPROGRESS;
            valid &= results[i].status == ITEM && results[i].reserved == 0;
            if (ITEM != 0)
                valid &= memcmp(&results[i].statbuf, &ZERO, sizeof(ZERO)) == 0;
            else
                valid &= S_ISREG(results[i].statbuf.st_mode) && results[i].statbuf.st_uid == 1000;
            if (!valid) printf("FAIL batch item=%zu status=%d expected=%d\n", i, results[i].status, ITEM);
        }
    } else {
        if (argc != 5) return 2;
        bool const ACCESS = strcmp(operation, "exists") == 0 || strcmp(operation, "readable") == 0 ||
                            strcmp(operation, "searchable") == 0 || strcmp(operation, "nofollow-exists") == 0;
        bool const OPEN_DIRECTORY = strcmp(operation, "open-directory") == 0;
        if (OPEN_DIRECTORY) {
            int fd = open(argv[3], O_RDONLY | O_DIRECTORY);
            result = fd < 0 ? -1 : close(fd);
        } else if (strcmp(operation, "create") == 0) {
            umask(0);
            int fd = open(argv[3], O_CREAT | O_EXCL | O_WRONLY, 0600);
            result = fd < 0 ? -1 : close(fd);
        } else if (strcmp(operation, "nofollow-exists") == 0)
            result = faccessat(AT_FDCWD, argv[3], F_OK, AT_SYMLINK_NOFOLLOW);
        else if (ACCESS)
            result = access(argv[3], strcmp(operation, "readable") == 0 ? R_OK : strcmp(operation, "searchable") == 0 ? X_OK : F_OK);
        else if (strcmp(operation, "link") == 0)
            result = lstat(argv[3], &output);
        else
            result = stat(argv[3], &output);
        int const ERROR = result < 0 ? errno : 0;
        valid = ERROR == EXPECTED;
        if (!ACCESS && !OPEN_DIRECTORY && strcmp(operation, "create") != 0) {
            if (ERROR != 0)
                valid &= memcmp(&output, &BEFORE, sizeof(output)) == 0;
            else if (strcmp(operation, "link") == 0)
                valid &= S_ISLNK(output.st_mode);
            else if (strcmp(operation, "directory") == 0)
                valid &= S_ISDIR(output.st_mode);
            else
                valid &= S_ISREG(output.st_mode) && output.st_uid == 1000 && (output.st_mode & 0777) == 0;
        }
        result = -ERROR;
    }
    printf("%s profile=%s operation=%s result=%d expected=%d\n", valid ? "PASS" : "FAIL", argv[1], operation, result, -EXPECTED);
    return valid ? 0 : 1;
}
