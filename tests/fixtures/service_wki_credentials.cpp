// Strict WKI credential round-trip fixture. A padded copy also exercises the
// receiver's file-backed ELF construction path. An optional final pathname
// selects a separate payload, allowing a custom remote PT_INTERP fixture.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <string.h>
#include <sys/process.h>
#include <sys/utsname.h>
#include <unistd.h>

struct CallerState {
    uid_t uid[3]{};
    gid_t gid[3]{};
    gid_t groups[8]{};
    int group_count = -1;
    int64_t no_new_privileges = -1;
    int64_t ceiling = -1;
    int64_t grants = -1;
    int64_t dumpable = -1;
    pid_t pid = -1;
    char executable[512]{};

    bool capture() {
        using Control = ker::abi::process::CredentialControl;
        group_count = getgroups(8, groups);
        no_new_privileges = ker::process::prctl(static_cast<int>(Control::GET_NO_NEW_PRIVILEGES), 0, 0, 0, 0);
        ceiling = ker::process::prctl(static_cast<int>(Control::GET_AUTHORITY_CEILING), 0, 0, 0, 0);
        grants = ker::process::prctl(static_cast<int>(Control::GET_SERVICE_PRIVILEGES), 0, 0, 0, 0);
        dumpable = ker::process::prctl(3, 0, 0, 0, 0);
        pid = getpid();
        auto length = readlink("/proc/self/exe", executable, sizeof(executable) - 1);
        return getresuid(&uid[0], &uid[1], &uid[2]) == 0 && getresgid(&gid[0], &gid[1], &gid[2]) == 0 && group_count >= 0 &&
               no_new_privileges >= 0 && ceiling >= 0 && grants >= 0 && dumpable >= 0 && length > 0 && length < sizeof(executable) - 1;
    }
    bool operator==(const CallerState& other) const {
        return !memcmp(uid, other.uid, sizeof(uid)) && !memcmp(gid, other.gid, sizeof(gid)) && group_count == other.group_count &&
               !memcmp(groups, other.groups, sizeof(groups)) && no_new_privileges == other.no_new_privileges && ceiling == other.ceiling &&
               grants == other.grants && dumpable == other.dumpable && pid == other.pid && !strcmp(executable, other.executable);
    }
};

auto main(int argc, char** argv) -> int {
    if (argc >= 2 && strcmp(argv[1], "--report") == 0) {
        uid_t ur, ue, us;
        gid_t gr, ge, gs;
        utsname identity{};
        if (getresuid(&ur, &ue, &us) != 0 || getresgid(&gr, &ge, &gs) != 0 || uname(&identity) != 0) return 2;
        printf("%s uid=%u/%u/%u gid=%u/%u/%u groups=%d dumpable=%lld\n", identity.nodename, ur, ue, us, gr, ge, gs, getgroups(0, nullptr),
               static_cast<long long>(ker::process::prctl(3, 0, 0, 0, 0)));
        return 0;
    }
    if ((argc != 4 && argc != 5) || geteuid() != 0 || (strcmp(argv[2], "root") != 0 && strcmp(argv[2], "user") != 0)) return 2;
    if (setgroups(0, nullptr) != 0) return 2;
    if (ker::process::setwkitarget(argv[1], strlen(argv[1]), ker::process::WKI_TARGET_FLAG_STRICT) != 0) return 2;
    if (strcmp(argv[2], "user") == 0) {
        if (setgid(1000) != 0 || setuid(1000) != 0) return 2;
        // Refusal must retain the dumpability cleared by this ID drop.
        // A successful readable plain exec resets it through prepare_exec.
    }
    utsname sender{};
    char path[512]{};
    const char* executable = argc == 5 ? argv[4] : argv[0];
    if (uname(&sender) != 0 || executable[0] != '/') return 2;
    int length = strncmp(executable, "/wki/", 5) == 0 ? snprintf(path, sizeof(path), "%s", executable)
                                                      : snprintf(path, sizeof(path), "/wki/%s%s", sender.nodename, executable);
    if (length <= 0 || static_cast<size_t>(length) >= sizeof(path)) return 2;
    char report[] = "--report";
    char padding[16385]{};
    bool fragmented = strcmp(argv[3], "fragment") == 0;
    if (fragmented) memset(padding, 'x', sizeof(padding) - 1);
    char* arguments[] = {path, report, fragmented ? padding : nullptr, nullptr};
    CallerState before{}, after{};
    int sentinel[2];
    if (!before.capture() || pipe(sentinel) != 0) return 2;
    bool prepared =
        fcntl(sentinel[0], F_SETFD, FD_CLOEXEC) == 0 && fcntl(sentinel[1], F_SETFD, FD_CLOEXEC) == 0 && write(sentinel[1], "S", 1) == 1;
    if (!prepared) {
        close(sentinel[0]);
        close(sentinel[1]);
        return 2;
    }
    execv(path, arguments);
    int error = errno;
    bool unchanged =
        after.capture() && before == after && fcntl(sentinel[0], F_GETFD) == FD_CLOEXEC && fcntl(sentinel[1], F_GETFD) == FD_CLOEXEC;
    close(sentinel[1]);
    char byte = 0;
    bool data_preserved = read(sentinel[0], &byte, 1) == 1 && byte == 'S';
    close(sentinel[0]);
    if (!unchanged || !data_preserved) {
        puts("FAIL: denied remote exec changed caller credentials, image or descriptors");
        return 2;
    }
    printf("remote exec rejected: errno=%d\n", error);
    return error;
}
