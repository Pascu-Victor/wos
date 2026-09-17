// Local permission checks only; requests cannot successfully reconfigure a NIC.
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/process.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <wos/netctl.h>

namespace {
int profile(unsigned mode) {
    if (mode == 1 && setuid(1000)) return 90;
    if (mode == 2 && ker::process::prctl(0x574f5302, 0, 0, 0, 0)) return 90;
    if (mode >= 3 && (ker::process::prctl(38, 1, 0, 0, 0) || ker::process::prctl(0x574f5303, mode == 3 ? 1 : 2, 0, 0, 0) ||
                      setuid(mode == 3 ? 101 : 102)))
        return 90;
    wos_net_if_info before[16]{}, after[16]{};
    size_t count = 16;
    if (wos_net_if_list(before, &count) || count < 2 || count > 16) return 91;
    wos_net_link_set_req req{};
    req.ifindex = before[0].ifindex;
    req.fields = WOS_NET_LINK_SET_MTU | WOS_NET_LINK_SET_TXQLEN | WOS_NET_LINK_SET_NAME;
    req.mtu = before[0].mtu == 1400 ? 1300 : 1400;
    req.tx_queue_len = before[0].tx_queue_len + 1;
    strcpy(req.new_name, before[1].name);
    const bool admitted = mode == 0 || mode == 4;
    errno = 0;
    if (wos_net_link_set(&req) != -1 || errno != (admitted ? EEXIST : EPERM)) return 92;
    req.mtu = 0;
    errno = 0;
    if (wos_net_link_set(&req) != -1 || errno != (admitted ? EINVAL : EPERM)) return 93;
    size_t after_count = 16;
    if (wos_net_if_list(after, &after_count) || after_count != count || memcmp(before, after, count * sizeof(before[0]))) return 94;
    errno = 0;
    int fd = socket(AF_INET, SOCK_RAW, 1);
    if (admitted) {
        if (fd < 0) return 95;
        close(fd);
    } else if (fd != -1 || errno != EPERM) {
        if (fd >= 0) close(fd);
        return 95;
    }
    fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return 96;
    close(fd);
    return 0;
}
}  // namespace

int main() {
    if (geteuid() != 0) return 90;
    unsigned failures = 0;
    for (unsigned mode = 0; mode < 5; ++mode) {
        const pid_t child = fork();
        if (child == 0) _exit(profile(mode));
        int status = -1;
        if (child < 0 || waitpid(child, &status, 0) != child || status != 0) ++failures;
        printf("network profile=%u status=%d\n", mode, status);
    }
    printf("network policy: profiles=5 failures=%u\n", failures);
    return failures ? 1 : 0;
}
