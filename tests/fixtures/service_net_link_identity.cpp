// Functional mapped link/MAC checks on the fixed disposable modern-virtio topology.
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/process.h>
#include <sys/wait.h>
#include <unistd.h>
#include <wos/netctl.h>

namespace {
int profile(unsigned mode, unsigned index, int expected_owner_error, int report) {
    if (mode == 1 && (setgid(1000) || setuid(1000))) return 90;
    if (mode == 2 && ker::process::prctl(0x574f5302, 0, 0, 0, 0)) return 90;
    if (mode >= 3) {
        const unsigned id = mode == 3 ? 101 : 102;
        if (ker::process::prctl(38, 1, 0, 0, 0) || ker::process::prctl(0x574f5303, mode == 3 ? 1 : 2, 0, 0, 0) || setgid(id) || setuid(id))
            return 90;
        if (mode == 5 && ker::process::prctl(0x574f5302, 0, 0, 0, 0)) return 90;
    }
    wos_net_if_info before[16]{}, after[16]{}, wanted[16]{};
    size_t count = 16;
    if (wos_net_if_list(before, &count) || count > 16 || index >= count) return 91;
    const bool admitted = mode == 0 || mode == 4;
    const int denied_error = admitted ? ENOTSUP : EPERM;
    const int same_error = admitted ? expected_owner_error : EPERM;
    // Fixed disposable-node topology: owner eth0 is 52:54:00:00:00:00.
    const unsigned char owner_mac[6]{0x52, 0x54, 0, 0, 0, 0};
    wos_net_link_set_req request{};
    request.ifindex = before[index].ifindex;
    request.fields =
        WOS_NET_LINK_SET_NAME | WOS_NET_LINK_SET_FLAGS | WOS_NET_LINK_SET_MTU | WOS_NET_LINK_SET_TXQLEN | WOS_NET_LINK_SET_HWADDR;
    strcpy(request.new_name, "kt-link-commit");
    request.flags = before[index].flags | 1U | 0x100U;
    request.flag_mask = 1U | 0x100U;
    request.mtu = 1400;
    request.tx_queue_len = 27;
    request.hwaddr_len = 6;
    memcpy(request.hwaddr, owner_mac, 6);
    request.hwaddr[5] = 37;
    errno = 0;
    const int denied = wos_net_link_set(&request), denied_errno = errno;
    size_t after_count = 16;
    bool preserved = !wos_net_if_list(after, &after_count) && after_count == count && !memcmp(before, after, count * sizeof(before[0]));
    unsigned char legacy[40]{};
    memcpy(legacy, before[index].name, sizeof(before[index].name));
    const unsigned short family = 1;
    memcpy(legacy + 16, &family, sizeof(family));
    memcpy(legacy + 18, request.hwaddr, 6);
    errno = 0;
    const int ioctl_result = ioctl(-1, 0x8924, legacy), ioctl_errno = errno;
    after_count = 16;
    preserved &= !wos_net_if_list(after, &after_count) && after_count == count && !memcmp(before, after, count * sizeof(before[0]));
    memcpy(request.hwaddr, owner_mac, 6);
    errno = 0;
    const int same = wos_net_link_set(&request), same_errno = errno;
    memcpy(wanted, before, sizeof(before));
    if (same_error == 0) {
        strcpy(wanted[index].name, request.new_name);
        wanted[index].flags |= 0x100U;
        wanted[index].mtu = request.mtu;
        wanted[index].tx_queue_len = request.tx_queue_len;
        memcpy(wanted[index].addr, owner_mac, 6);
    }
    after_count = 16;
    const bool committed =
        !wos_net_if_list(after, &after_count) && after_count == count && !memcmp(wanted, after, count * sizeof(before[0]));
    int restore = 0;
    if (same == 0) {
        request.fields &= ~WOS_NET_LINK_SET_HWADDR;
        strcpy(request.new_name, before[index].name);
        request.flags = before[index].flags;
        request.mtu = before[index].mtu;
        request.tx_queue_len = before[index].tx_queue_len;
        restore = wos_net_link_set(&request);
    }
    memcpy(wanted, before, sizeof(before));
    // Authorized MAC assignment adopts the owner's requested address in this
    // disposable proxy. Restore local ancillary fields; owner MAC never changes.
    if (same_error == 0) memcpy(wanted[index].addr, owner_mac, 6);
    after_count = 16;
    const bool restored =
        !restore && !wos_net_if_list(after, &after_count) && after_count == count && !memcmp(wanted, after, count * sizeof(before[0]));
    dprintf(report, "profile=%u denied=%d/%d legacy=%d/%d expected=%d same=%d/%d expected_same=%d committed=%d preserved=%d restored=%d\n",
            mode, denied, denied_errno, ioctl_result, ioctl_errno, denied_error, same, same_errno, same_error, committed, preserved,
            restored);
    return denied == -1 && denied_errno == denied_error && ioctl_result == -1 && ioctl_errno == denied_error &&
                   same == (same_error ? -1 : 0) && (!same_error || same_errno == same_error) && committed && preserved && restored
               ? 0
               : 92;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc != 4 || geteuid() != 0) return 90;
    char hostname[64]{};
    if (gethostname(hostname, sizeof(hostname)) || strcmp(hostname, argv[3])) return 93;
    const int report = open(argv[2], O_CREAT | O_EXCL | O_WRONLY, 0600);
    if (report < 0) return 94;
    wos_net_if_info interfaces[16]{};
    size_t count = 16;
    if (wos_net_if_list(interfaces, &count) || count > 16) return 95;
    if (!strcmp(argv[1], "--snapshot")) {
        const auto* bytes = reinterpret_cast<const unsigned char*>(interfaces);
        for (size_t i = 0; i < count * sizeof(interfaces[0]); ++i) dprintf(report, "%02x", bytes[i]);
        dprintf(report, "\n");
        const int status = fsync(report);
        close(report);
        return status ? 96 : 0;
    }
    unsigned index = static_cast<unsigned>(count);
    for (unsigned i = 0; i < count; ++i) {
        if (!strncmp(interfaces[i].name, "wki-wos-0", 9) && (interfaces[i].flags & 1U)) {
            index = i;
            break;
        }
    }
    if (index == count) {
        dprintf(report, "FAIL: no already-UP owner proxy\n");
        close(report);
        return 97;
    }
    dprintf(report, "node=%s proxy=%s\n", hostname, interfaces[index].name);
    unsigned failures = 0;
    for (unsigned mode = 0; mode < 6; ++mode) {
        pid_t child = fork();
        if (child == 0) _exit(profile(mode, index, atoi(argv[1]), report));
        int status = -1;
        if (child < 0 || waitpid(child, &status, 0) != child || status != 0) ++failures;
        dprintf(report, "wait profile=%u status=%d\n", mode, status);
    }
    dprintf(report, "%s: remote link profiles=6 failures=%u\n", failures ? "FAIL" : "PASS", failures);
    const int sync_status = fsync(report);
    close(report);
    return failures || sync_status ? 1 : 0;
}
