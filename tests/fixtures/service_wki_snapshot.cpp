// Complete credential transport and irreversible-drop checks on the actual
// receiver. Use a padded copy to select file-backed ELF construction.
#include <errno.h>
#include <grp.h>
#include <spawn.h>
#include <stdio.h>
#include <string.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
constexpr int GET_CEILING = static_cast<int>(ker::abi::process::CredentialControl::GET_AUTHORITY_CEILING);
constexpr int DROP_CEILING = static_cast<int>(ker::abi::process::CredentialControl::DROP_AUTHORITY_CEILING);
constexpr int SET_SERVICE = static_cast<int>(ker::abi::process::CredentialControl::SET_SERVICE_PRIVILEGES);
constexpr int GET_SERVICE = static_cast<int>(ker::abi::process::CredentialControl::GET_SERVICE_PRIVILEGES);
constexpr unsigned ALL_AUTHORITY = ker::abi::process::CREDENTIAL_AUTHORITY_ALL;
constexpr unsigned SERVICE_AUTHORITY = ker::abi::process::service_privilege_ceiling(3);
auto control(int operation, unsigned value = 0) -> long long { return ker::process::prctl(operation, value, 0, 0, 0); }
auto report(const char* profile) -> int {
    uid_t ur, ue, us;
    gid_t gr, ge, gs, groups[8]{};
    utsname node{};
    if (getresuid(&ur, &ue, &us) || getresgid(&gr, &ge, &gs) || uname(&node)) return 2;
    const bool root = strcmp(profile, "root") == 0;
    const bool service = strcmp(profile, "service") == 0;
    const unsigned uid = root ? 0 : 1000;
    const unsigned gid = root ? 0 : 2000;
    const unsigned ceiling = service ? SERVICE_AUTHORITY : 0;
    const unsigned grants = root ? 256 : 259;
    bool valid = ur == uid && ue == uid && us == uid && gr == gid && ge == gid && gs == gid && getgroups(8, groups) == 8 &&
                 control(39) == 1 && control(3) == 1 && control(GET_CEILING) == ceiling && control(GET_SERVICE) == grants;
    for (unsigned i = 0; i < 8; ++i) valid &= groups[i] == 3000 + i;
    // Queries above verify the transported state before these rejected changes.
    valid &= control(DROP_CEILING, ALL_AUTHORITY) == -EPERM;
    valid &= control(SET_SERVICE, 0) == 0;
    valid &= control(SET_SERVICE, 3) == -EPERM;
    valid &= control(38, 0) == -EINVAL;
    if (!root) {
        errno = 0;
        valid &= setuid(0) == -1 && errno == EPERM;
    }
    valid &= control(GET_CEILING) == ceiling && control(GET_SERVICE) == 256 && control(39) == 1;
    printf("%s profile=%s snapshot=%s no-regain=%s\n", node.nodename, profile, valid ? "exact" : "FAIL", valid ? "yes" : "FAIL");
    return valid ? 0 : 1;
}
}  // namespace

auto main(int argc, char** argv) -> int {
    if (argc == 3 && strcmp(argv[1], "--mkdir-denial") == 0) {
        struct stat st{};
        errno = 0;
        if (mkdir(argv[2], 0755) != -1 || errno != ENOSYS) return 2;
        errno = 0;
        if (stat(argv[2], &st) != -1 || errno != ENOENT) return 2;
        puts("PASS: unsupported FAT mkdir left path absent");
        return 0;
    }
    if (argc >= 3 && strcmp(argv[1], "--report") == 0) return report(argv[2]);
    if ((argc != 5 && argc != 6) || geteuid() != 0) return 2;
    const bool root = strcmp(argv[2], "root") == 0;
    const bool service = strcmp(argv[2], "service") == 0;
    if (!root && !service && strcmp(argv[2], "inert") != 0) return 2;
    const bool local_boot = strcmp(argv[4], "strict-local-boot") == 0;
    const bool strict = local_boot || strcmp(argv[4], "strict") == 0;
    const bool local = strcmp(argv[4], "local") == 0;
    if (!strict && !local && strcmp(argv[4], "optional") != 0) return 2;
    const unsigned flags = local ? ker::process::WKI_TARGET_FLAG_LOCAL : (strict ? ker::process::WKI_TARGET_FLAG_STRICT : 0);
    if (ker::process::setwkitarget(local ? nullptr : argv[1], local ? 0 : strlen(argv[1]), flags) != 0) return 2;
    if (local_boot && ker::abi::vfs::wki_rule_add_vfs("/boot", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL) != 0) return 2;
    gid_t groups[8]{3000, 3001, 3002, 3003, 3004, 3005, 3006, 3007};
    if (setgroups(8, groups) || control(38, 1) || control(SET_SERVICE, root ? 0 : 3)) return 2;
    if (!root && (setgid(2000) || setuid(1000))) return 2;
    if (control(DROP_CEILING, service ? SERVICE_AUTHORITY : 0) || control(4, 1)) return 2;
    utsname sender{};
    char path[512]{};
    if (uname(&sender) || argv[0][0] != '/') return 2;
    const char* executable = argc == 6 ? argv[5] : argv[0];
    if (executable[0] != '/') return 2;
    int length = local || strncmp(executable, "/wki/", 5) == 0 ? snprintf(path, sizeof(path), "%s", executable)
                                                               : snprintf(path, sizeof(path), "/wki/%s%s", sender.nodename, executable);
    if (length <= 0 || static_cast<size_t>(length) >= sizeof(path)) return 2;
    char report_arg[] = "--report";
    char padding[16385]{};
    const bool fragmented = strcmp(argv[3], "fragment") == 0;
    if (fragmented) memset(padding, 'x', sizeof(padding) - 1);
    char* arguments[]{path, report_arg, argv[2], fragmented ? padding : nullptr, nullptr};
    if (local) {
        pid_t pid = -1;
        int error = posix_spawn(&pid, path, nullptr, nullptr, arguments, environ);
        if (error != 0) {
            printf("remote exec rejected: errno=%d\n", error);
            return error;
        }
        int status = 0;
        if (waitpid(pid, &status, 0) != pid || !WIFEXITED(status)) return 2;
        return WEXITSTATUS(status);
    }
    execv(path, arguments);
    int error = errno;
    printf("remote exec rejected: errno=%d\n", error);
    return error;
}
