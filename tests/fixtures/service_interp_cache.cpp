// WOS SPAWN uses the kernel's interpreter construction cache.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

struct CallerState {
    uid_t uid[3]{};
    gid_t gid[3]{};
    gid_t groups[8]{};
    int group_count = -1;
    int64_t no_new_privileges = -1;
    int64_t ceiling = -1;
    int64_t grants = -1;
    pid_t pid = -1;
    char executable[512]{};

    bool capture() {
        using Control = ker::abi::process::CredentialControl;
        group_count = getgroups(8, groups);
        no_new_privileges = ker::process::prctl(static_cast<int>(Control::GET_NO_NEW_PRIVILEGES), 0, 0, 0, 0);
        ceiling = ker::process::prctl(static_cast<int>(Control::GET_AUTHORITY_CEILING), 0, 0, 0, 0);
        grants = ker::process::prctl(static_cast<int>(Control::GET_SERVICE_PRIVILEGES), 0, 0, 0, 0);
        pid = getpid();
        auto length = readlink("/proc/self/exe", executable, sizeof(executable) - 1);
        return getresuid(&uid[0], &uid[1], &uid[2]) == 0 && getresgid(&gid[0], &gid[1], &gid[2]) == 0 && group_count >= 0 &&
               no_new_privileges >= 0 && ceiling >= 0 && grants >= 0 && length > 0 && length < sizeof(executable) - 1;
    }
    bool operator==(const CallerState& other) const {
        return !memcmp(uid, other.uid, sizeof(uid)) && !memcmp(gid, other.gid, sizeof(gid)) && group_count == other.group_count &&
               !memcmp(groups, other.groups, sizeof(groups)) && no_new_privileges == other.no_new_privileges && ceiling == other.ceiling &&
               grants == other.grants && pid == other.pid && !strcmp(executable, other.executable);
    }
};

static int wait_child(pid_t pid) {
    int status = 0;
    for (unsigned i = 0; i < 10000; ++i) {
        auto result = waitpid(pid, &status, WNOHANG);
        if (result == pid) return WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 0 : 2;
        if (result < 0 && errno != EINTR) return 2;
        usleep(1000);
    }
    kill(pid, SIGKILL);
    for (unsigned i = 0; i < 1000; ++i) {
        if (waitpid(pid, &status, WNOHANG) == pid) break;
        usleep(1000);
    }
    return 2;
}

static int spawn_payload(const char* path, int expected_error = 0) {
    CallerState before{}, after{};
    if (!before.capture()) return 2;
    int sentinel[2];
    if (pipe(sentinel) != 0) return 2;
    bool prepared = fcntl(sentinel[0], F_SETFD, FD_CLOEXEC) == 0 && write(sentinel[1], "S", 1) == 1;
    const char* args[] = {path, nullptr};
    const char* environment[] = {nullptr};
    int64_t pid = prepared ? static_cast<int64_t>(ker::process::spawn(path, args, environment, nullptr)) : -1;
    int result = pid > 0 ? wait_child(static_cast<pid_t>(pid)) : 1;
    close(sentinel[1]);
    char byte = 0;
    bool intact = prepared && after.capture() && before == after && fcntl(sentinel[0], F_GETFD) == FD_CLOEXEC &&
                  read(sentinel[0], &byte, 1) == 1 && byte == 'S';
    close(sentinel[0]);
    if (!intact) {
        puts("FAIL: interpreter construction changed caller credentials, image or descriptor");
        return 2;
    }
    if (expected_error != 0 && pid != -expected_error) {
        printf("FAIL: interpreter denial expected=%d actual=%lld\n", expected_error, static_cast<long long>(pid));
        return 2;
    }
    return result;
}

static int execve_denial(const char* path) {
    int report[2];
    if (pipe(report) != 0) return 2;
    pid_t child = fork();
    if (child == 0) {
        close(report[0]);
        CallerState before{}, after{};
        if (fcntl(report[1], F_SETFD, FD_CLOEXEC) != 0 || !before.capture()) _exit(2);
        char* args[] = {const_cast<char*>(path), nullptr};
        execv(path, args);
        bool intact = errno == EACCES && after.capture() && before == after && fcntl(report[1], F_GETFD) == FD_CLOEXEC;
        if (!intact || write(report[1], "D", 1) != 1) _exit(2);
        _exit(0);
    }
    close(report[1]);
    char result = 0;
    bool ok = child > 0 && wait_child(child) == 0 && read(report[0], &result, 1) == 1 && result == 'D';
    close(report[0]);
    return ok ? 0 : 2;
}

int main(int argc, char** argv) {
    if (argc != 2 || getuid() != 0) return 2;
    if (ker::process::setwkitarget("", 0, ker::process::WKI_TARGET_FLAG_LOCAL) != 0) return 2;
    if (spawn_payload(argv[1]) != 0) {
        puts("FAIL: root cache warmup");
        return 2;
    }
    // Equal size is not an interpreter version. Corrupt the source after
    // warmup and restore it regardless of the construction result.
    const char* interpreter = "/tmp/goal19-interp-private/ld.so";
    for (unsigned round = 0; round < 8; ++round) {
        int source = open(interpreter, O_RDWR);
        unsigned char magic = 0;
        unsigned char invalid = 0;
        if (source < 0 || pread(source, &magic, 1, 0) != 1 || pwrite(source, &invalid, 1, 0) != 1) return 2;
        close(source);
        int corrupted_result = spawn_payload(argv[1]);
        source = open(interpreter, O_WRONLY);
        if (source < 0 || pwrite(source, &magic, 1, 0) != 1) return 2;
        close(source);
        if (corrupted_result != 1 || spawn_payload(argv[1]) != 0) {
            puts("FAIL: same-size interpreter mutation bypassed cache validation");
            return 2;
        }
    }
    if (chmod(interpreter, 0644) != 0) return 2;
    for (unsigned round = 0; round < 8; ++round) {
        if (spawn_payload(argv[1], EACCES) != 1 || execve_denial(argv[1]) != 0) {
            puts("FAIL: root executed a cached interpreter without any execute bit");
            return 2;
        }
    }
    if (chmod(interpreter, 0700) != 0) return 2;
    pid_t child = fork();
    if (child < 0) return 2;
    if (child == 0) {
        if (seteuid(1000) != 0) _exit(2);
        int fd = open("/tmp/goal19-interp-private/ld.so", O_RDONLY);
        if (fd >= 0) {
            close(fd);
            _exit(2);
        }
        if (errno != EACCES) _exit(2);
        struct stat st{};
        if (stat("/tmp/goal19-interp-private/ld.so", &st) != 0) _exit(2);
        for (unsigned round = 0; round < 8; ++round) {
            int result = spawn_payload(argv[1]);
            if (result != 1) _exit(result == 0 ? 1 : 2);
        }
        _exit(0);
    }
    if (wait_child(child) != 0) {
        puts("FAIL: unreadable cached interpreter executed");
        return 2;
    }
    if (chmod("/tmp/goal19-interp-private/ld.so", 0644) != 0) return 2;
    child = fork();
    if (child < 0) return 2;
    if (child == 0) {
        if (seteuid(1000) != 0) _exit(2);
        for (unsigned round = 0; round < 8; ++round)
            if (spawn_payload(argv[1], EACCES) != 1 || execve_denial(argv[1]) != 0) _exit(2);
        _exit(0);
    }
    if (wait_child(child) != 0) {
        puts("FAIL: readable non-executable interpreter was not rejected");
        return 2;
    }
    if (chmod(interpreter, 0755) != 0) return 2;
    child = fork();
    if (child < 0) return 2;
    if (child == 0) {
        if (seteuid(1000) != 0) _exit(2);
        _exit(spawn_payload(argv[1]));
    }
    if (wait_child(child) != 0) {
        puts("FAIL: readable interpreter control");
        return 2;
    }
    puts(
        "PASS: 42 spawn and 16 replacement attempts preserve caller credentials/image/CLOEXEC/data; corrupt/recover, cached read and "
        "execute denials, root/user no-execute-bit denial and executable control");
    return 0;
}
