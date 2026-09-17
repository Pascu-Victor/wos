// Functional image-list access before and after target dumpability changes.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/process.h>
#include <sys/wait.h>
#include <unistd.h>

#include "../../modules/kern/src/abi/ptrace.hpp"

namespace {
namespace trace = ker::abi::ptrace;
int64_t call(trace::request op, pid_t pid, void* data = nullptr) {
    return ker::process::ptrace(static_cast<uint64_t>(op), pid, 0, reinterpret_cast<uint64_t>(data));
}
bool byte(int fd, char expected) {
    char value = 0;
    ssize_t count;
    do {
        count = read(fd, &value, 1);
    } while (count < 0 && errno == EINTR);
    return count == 1 && value == expected;
}
bool query(pid_t pid, bool catalog, int expected, bool invalid = false) {
    if (catalog) {
        trace::ImageCatalogRecord records[32];
        memset(records, 0xa5, sizeof(records));
        trace::ImageCatalogRecord before[32];
        memcpy(before, records, sizeof(records));
        trace::ImageCatalogList list{trace::IMAGE_CATALOG_VERSION + (invalid ? 1U : 0U),
                                     sizeof(records[0]),
                                     records,
                                     32,
                                     55,
                                     trace::image_snapshot_status::TRUNCATED,
                                     17};
        auto const original = list;
        int64_t result = call(trace::request::GET_IMAGE_CATALOG, pid, &list);
        if (expected) return result == -expected && !memcmp(&original, &list, sizeof(list)) && !memcmp(before, records, sizeof(records));
        return result == 0 && list.count > 0 && list.count <= 32 && list.reserved == 0 && records[0].path[0] != 0 &&
               records[0].image_start < records[0].image_end;
    }
    trace::ImageRecord records[32];
    memset(records, 0xa5, sizeof(records));
    trace::ImageRecord before[32];
    memcpy(before, records, sizeof(records));
    trace::ImageList list{records, 32, 55};
    auto const original = list;
    int64_t result = call(trace::request::GET_IMAGES, pid, &list);
    if (expected) return result == -expected && !memcmp(&original, &list, sizeof(list)) && !memcmp(before, records, sizeof(records));
    return result == 0 && list.count > 0 && list.count <= 32 && records[0].path[0] != 0;
}
}  // namespace

int main(int argc, char** argv) {
    char node[64]{};
    if (argc != 3 || geteuid() != 0 || gethostname(node, sizeof(node)) || strcmp(node, argv[2])) return 2;
    int report = open(argv[1], O_CREAT | O_EXCL | O_WRONLY, 0600);
    if (report < 0) return 3;
    int ready[2], command[2];
    if (pipe(ready) || pipe(command)) return 4;
    alarm(45);
    pid_t target = fork();
    if (target < 0) return 5;
    if (target == 0) {
        close(ready[0]);
        close(command[1]);
        close(report);
        if (setgroups(0, nullptr) || setgid(2000) || setuid(1000) || ker::process::prctl(4, 1, 0, 0, 0)) _exit(10);
        if (write(ready[1], "R", 1) != 1 || !byte(command[0], 'D') || ker::process::prctl(4, 0, 0, 0, 0) || write(ready[1], "D", 1) != 1 ||
            !byte(command[0], 'X'))
            _exit(11);
        _exit(0);
    }
    close(ready[1]);
    close(command[0]);
    unsigned checks = 0, failures = 0;
    auto check = [&](const char* name, bool ok) {
        ++checks;
        if (!ok) ++failures;
        dprintf(report, "node=%s op=%s preserved=%d\n", node, name, ok);
    };
    check("target-ready", byte(ready[0], 'R'));
    check("seize", call(trace::request::SEIZE, target) == 0);
    check("root-legacy", query(target, false, 0));
    check("root-catalog", query(target, true, 0));
    check("invalid-version", query(target, true, EINVAL, true));
    check("drop-tracer", !setgroups(0, nullptr) && !setgid(2000) && !setuid(1000));
    check("user-legacy", query(target, false, 0));
    check("user-catalog", query(target, true, 0));
    check("target-revoke", write(command[1], "D", 1) == 1 && byte(ready[0], 'D'));
    check("denied-legacy", query(target, false, EPERM));
    check("denied-catalog", query(target, true, EPERM));
    check("detach", call(trace::request::DETACH, target) == 0);
    check("release-target", write(command[1], "X", 1) == 1);
    close(command[1]);
    close(ready[0]);
    int status = -1;
    pid_t waited;
    do {
        waited = waitpid(target, &status, 0);
    } while (waited < 0 && errno == EINTR);
    check("target-exit", waited == target && status == 0);
    dprintf(report, "%s: ptrace-images checks=%u failures=%u\n", failures ? "FAIL" : "PASS", checks, failures);
    int const synced = fsync(report);
    close(report);
    return failures || synced || checks != 14 ? 1 : 0;
}
