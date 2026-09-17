// Functional IPv4 authorization checks on the disposable node's loopback interface.
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/process.h>
#include <sys/wait.h>
#include <unistd.h>
#include <wos/netctl.h>

namespace {
struct Addresses {
    wos_net_addr_info rows[8]{};
    size_t count = 0;
};

bool observe(unsigned index, Addresses& out) {
    wos_net_addr_info rows[256]{};
    size_t count = 256;
    if (wos_net_addr_list(rows, &count) || count > 256) return false;
    out = {};
    for (size_t i = 0; i < count; ++i) {
        if (rows[i].ifindex != index || rows[i].family != AF_INET) continue;
        if (out.count == 8) return false;
        out.rows[out.count++] = rows[i];
    }
    return true;
}

bool same(const Addresses& a, const Addresses& b) { return a.count == b.count && !memcmp(a.rows, b.rows, a.count * sizeof(a.rows[0])); }

int drop(unsigned mode) {
    if (mode == 1 && (setgid(1000) || setuid(1000))) return 90;
    if (mode == 2 && ker::process::prctl(0x574f5302, 0, 0, 0, 0)) return 90;
    if (mode >= 3) {
        unsigned id = mode == 3 ? 101 : 102;
        if (ker::process::prctl(38, 1, 0, 0, 0) || ker::process::prctl(0x574f5303, mode == 3 ? 1 : 2, 0, 0, 0) || setgid(id) || setuid(id))
            return 90;
        if (mode == 5 && ker::process::prctl(0x574f5302, 0, 0, 0, 0)) return 90;
    }
    return 0;
}

int profile(unsigned mode, unsigned index, const wos_net_addr_req& extra, int report) {
    if (drop(mode)) return 90;
    bool const admitted = mode == 0 || mode == 4;
    Addresses before{}, after{};
    if (!observe(index, before) || before.count == 0 || before.count == 8) return 91;
    unsigned failures = 0;
    auto check = [&](unsigned op, int result, int error, int expected, bool preserved) {
        bool ok = result == (expected ? -1 : 0) && (!expected || error == expected) && preserved;
        dprintf(report, "profile=%u op=%u result=%d/%d expected=%d preserved=%d\n", mode, op, result, error, expected, preserved);
        if (!ok) ++failures;
    };
    // Exercise the legacy writers without changing the node's primary loopback address.
    for (unsigned op = 0; op < 2; ++op) {
        unsigned char legacy[40]{};
        memcpy(legacy, "lo", 3);
        unsigned short family = AF_INET;
        memcpy(legacy + 16, &family, sizeof(family));
        if (op == 0)
            memcpy(legacy + 20, before.rows[0].local, 4);
        else {
            unsigned prefix = before.rows[0].prefix_len;
            uint32_t mask = htonl(prefix == 0 ? 0 : 0xffffffffU << (32 - prefix));
            memcpy(legacy + 20, &mask, sizeof(mask));
        }
        errno = 0;
        int result = ioctl(-1, op == 0 ? 0x8916 : 0x891c, legacy);
        int error = errno;
        check(op, result, error, admitted ? 0 : EPERM, observe(index, after) && same(before, after));
    }
    errno = 0;
    int result = wos_net_addr_set(&extra);
    int error = errno;
    bool observed = observe(index, after);
    bool expected_state = observed && same(before, after);
    if (admitted && observed && after.count == before.count + 1) {
        expected_state = !memcmp(before.rows, after.rows, before.count * sizeof(before.rows[0])) &&
                         after.rows[before.count].prefix_len == 32 && !memcmp(after.rows[before.count].local, extra.local, 4);
    }
    check(2, result, error, admitted ? 0 : EPERM, expected_state);
    Addresses const ADDED = after;
    errno = 0;
    result = wos_net_addr_set(&extra);
    error = errno;
    check(3, result, error, admitted ? EEXIST : EPERM, observe(index, after) && same(ADDED, after));
    errno = 0;
    result = wos_net_addr_del(&extra);
    error = errno;
    check(4, result, error, admitted ? 0 : EPERM, observe(index, after) && same(before, after));
    return failures ? 92 : 0;
}
}  // namespace

int main(int argc, char** argv) {
    char node[64]{};
    if (argc != 3 || geteuid() != 0 || gethostname(node, sizeof(node)) || strcmp(node, argv[2])) return 2;
    wos_net_if_info interfaces[32]{};
    size_t count = 32;
    if (wos_net_if_list(interfaces, &count) || count > 32) return 3;
    unsigned index = 0;
    for (size_t i = 0; i < count; ++i)
        if (!strcmp(interfaces[i].name, "lo")) index = interfaces[i].ifindex;
    Addresses baseline{};
    if (index == 0 || !observe(index, baseline) || baseline.count == 0 || baseline.count == 8) return 4;
    wos_net_addr_req extra{};
    extra.ifindex = index;
    extra.family = AF_INET;
    extra.prefix_len = 32;
    extra.local[0] = 127;
    extra.local[1] = 88;
    extra.local[2] = 19;
    extra.local[3] = 1;
    for (size_t i = 0; i < baseline.count; ++i)
        if (!memcmp(baseline.rows[i].local, extra.local, 4)) return 5;
    int const report = open(argv[1], O_CREAT | O_EXCL | O_WRONLY, 0600);
    if (report < 0) return 6;
    unsigned failures = 0;
    dprintf(report, "node=%s loopback=%u\n", node, index);
    for (unsigned mode = 0; mode < 6; ++mode) {
        pid_t child = fork();
        if (child == 0) _exit(profile(mode, index, extra, report));
        int status = -1;
        if (child < 0 || waitpid(child, &status, 0) != child || status != 0) ++failures;
        // Parent retains cleanup authority even when a child failed or dropped it.
        errno = 0;
        int cleanup = wos_net_addr_del(&extra);
        if (cleanup && errno != EADDRNOTAVAIL) ++failures;
        Addresses restored{};
        bool const RESTORED = observe(index, restored) && same(baseline, restored);
        if (!RESTORED) ++failures;
        dprintf(report, "wait profile=%u status=%d restored=%d\n", mode, status, RESTORED);
    }
    dprintf(report, "%s: IPv4 profiles=6 checks=30 failures=%u\n", failures ? "FAIL" : "PASS", failures);
    int const SYNC = fsync(report);
    close(report);
    return failures || SYNC ? 1 : 0;
}
