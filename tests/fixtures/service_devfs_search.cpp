// Native ancestor-search checks with local-submitter LOCAL/HOST routing rules.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
auto child_checks(const char* path, unsigned rules) -> bool {
    namespace abi = ker::abi::vfs;
    if (rules != 0 && abi::wki_rule_add_vfs("/dev", rules == 2 ? abi::WKI_VFS_ROUTE_HOST : abi::WKI_VFS_ROUTE_LOCAL)) return false;
    if (rules == 3 &&
        (abi::wki_rule_add_vfs("/dev/pts", abi::WKI_VFS_ROUTE_HOST) || abi::wki_rule_add_vfs("/unrelated", abi::WKI_VFS_ROUTE_HOST)))
        return false;
    using Control = ker::abi::process::CredentialControl;
    if (setgroups(0, nullptr) || setgid(2000) || setuid(1000) ||
        ker::process::prctl(static_cast<int>(Control::SET_NO_NEW_PRIVILEGES), 1, 0, 0, 0) ||
        ker::process::prctl(static_cast<int>(Control::DROP_AUTHORITY_CEILING), 0, 0, 0, 0))
        return false;
    auto report = [rules](const char* operation, bool denied, bool untouched = true) -> bool {
        bool const PASS = denied && untouched;
        printf("%s rules=%u operation=%s errno=%d untouched=%d\n", PASS ? "PASS" : "FAIL", rules, operation, errno, untouched);
        return PASS;
    };
    char output[32];
    memset(output, 'Z', sizeof(output));
    errno = 0;
    ssize_t const READ = readlink(path, output, sizeof(output));
    bool untouched = true;
    for (char c : output) untouched &= c == 'Z';
    bool valid = report("readlink", READ == -1 && errno == EACCES, untouched);
    struct stat metadata;
    memset(&metadata, 0x5A, sizeof(metadata));
    struct stat before;
    memcpy(&before, &metadata, sizeof(metadata));
    errno = 0;
    int const STAT = lstat(path, &metadata);
    valid &= report("lstat", STAT == -1 && errno == EACCES, memcmp(&metadata, &before, sizeof(metadata)) == 0);
    errno = 0;
    int const ACCESS = access(path, F_OK);
    valid &= report("access", ACCESS == -1 && errno == EACCES);
    errno = 0;
    int const FD = open(path, O_RDONLY | O_NOCTTY);
    valid &= report("open", FD == -1 && errno == EACCES);
    if (FD >= 0) close(FD);
    fflush(stdout);
    return valid;
}
}  // namespace

auto main() -> int {
    int const MASTER = posix_openpt(O_RDWR | O_NOCTTY);
    if (MASTER < 0 || grantpt(MASTER) || unlockpt(MASTER)) return 2;
    char path[128]{};
    char* name = ptsname(MASTER);
    struct stat before{};
    if (!name || snprintf(path, sizeof(path), "%s", name) >= static_cast<int>(sizeof(path)) || stat("/dev/pts", &before) ||
        chmod(path, 0666) || chmod("/dev/pts", 0700))
        return 2;
    bool valid = true;
    for (unsigned rules = 0; rules < 4; ++rules) {
        pid_t const CHILD = fork();
        if (CHILD == 0) _exit(child_checks(path, rules) ? 0 : 1);
        int status = 0;
        if (CHILD < 0 || waitpid(CHILD, &status, 0) != CHILD || !WIFEXITED(status) || WEXITSTATUS(status) != 0) valid = false;
    }
    bool const RESTORED = chmod("/dev/pts", before.st_mode & 07777) == 0;
    printf("%s: 16 local-submitter ancestor-search checks; restored=%d\n", valid && RESTORED ? "PASS" : "FAIL", RESTORED);
    close(MASTER);
    return valid && RESTORED ? 0 : 1;
}
