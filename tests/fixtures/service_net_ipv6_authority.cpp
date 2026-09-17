// Functional control checks on a disposable WOS VM; no socket traffic is sent.
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/process.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <wos/netctl.h>

#include <initializer_list>

namespace {
struct State {
    wos_net_addr_info addresses[16]{};
    wos_net_route_record routes[128]{};
    size_t address_count{}, route_count{};
};
auto observe(unsigned index, State& out) -> bool {
    wos_net_addr_info addresses[256]{};
    wos_net_route_record routes[128]{};
    size_t count = 256;
    out = {};
    if (wos_net_addr_list(addresses, &count) || count > 256) return false;
    for (size_t i = 0; i < count; ++i) {
        if (addresses[i].ifindex != index || addresses[i].family != AF_INET6) continue;
        if (out.address_count == 16) return false;
        out.addresses[out.address_count++] = addresses[i];
    }
    count = 128;
    if (wos_net_route_list(routes, &count) || count > 128) return false;
    for (size_t i = 0; i < count; ++i)
        if (routes[i].ifindex == index) out.routes[out.route_count++] = routes[i];
    return true;
}
auto same(const State& a, const State& b) -> bool {
    return a.address_count == b.address_count && a.route_count == b.route_count &&
           !memcmp(a.addresses, b.addresses, a.address_count * sizeof(a.addresses[0])) &&
           !memcmp(a.routes, b.routes, a.route_count * sizeof(a.routes[0]));
}
auto drop(unsigned mode) -> bool {
    if (setgroups(0, nullptr)) return false;
    if (mode == 1 && (setgid(1000) || setuid(1000))) return false;
    if (mode == 2 && ker::process::prctl(0x574f5302, 0, 0, 0, 0)) return false;
    if (mode >= 3) {
        unsigned const ID = mode == 3 ? 101 : 102;
        if (ker::process::prctl(38, 1, 0, 0, 0) || ker::process::prctl(0x574f5303, mode == 3 ? 1 : 2, 0, 0, 0) || setgid(ID) || setuid(ID))
            return false;
        if (mode == 5 && ker::process::prctl(0x574f5302, 0, 0, 0, 0)) return false;
    }
    return true;
}
auto profile(unsigned mode, const wos_net_addr_req& address, const wos_net_route_record& route, int report) -> int {
    alarm(30);
    if (!drop(mode)) return 90;
    bool const ADMITTED = mode == 0 || mode == 4;
    unsigned failures = 0, checks = 0;
    State baseline{}, current{}, after{};
    if (!observe(address.ifindex, baseline)) return 91;
    current = baseline;
    auto check = [&](const char* operation, int result, int error, int expected, bool state_ok) {
        bool const OK = result == (expected ? -1 : 0) && (!expected || error == expected) && state_ok;
        dprintf(report, "profile=%u op=%s result=%d errno=%d expected=%d state=%d ok=%d\n", mode, operation, result, error, expected,
                state_ok, OK);
        ++checks;
        failures += !OK;
    };
    for (int family : {AF_INET, AF_INET6}) {
        for (int type : {SOCK_DGRAM, SOCK_RAW}) {
            errno = 0;
            int const FD = socket(family, type, type == SOCK_DGRAM ? IPPROTO_UDP : family == AF_INET ? IPPROTO_ICMP : IPPROTO_ICMPV6);
            int const ERROR = errno;
            bool const CLOSED = FD < 0 || close(FD) == 0;
            check(family == AF_INET ? (type == SOCK_RAW ? "raw4" : "udp4") : (type == SOCK_RAW ? "raw6" : "udp6"), FD < 0 ? -1 : 0, ERROR,
                  type == SOCK_RAW && !ADMITTED ? EPERM : 0, CLOSED && observe(address.ifindex, after) && same(current, after));
        }
    }
    errno = 0;
    int result = wos_net_addr_set(&address);
    int error = errno;
    State expected = current;
    if (ADMITTED) {
        auto& row = expected.addresses[expected.address_count++];
        row.ifindex = address.ifindex;
        row.family = AF_INET6;
        row.prefix_len = address.prefix_len;
        row.flags = address.flags;
        strcpy(row.label, "lo");
        memcpy(row.local, address.local, 16);
        memcpy(row.address, address.local, 16);
    }
    check("addr-add", result, error, ADMITTED ? 0 : EPERM, observe(address.ifindex, after) && same(expected, after));
    current = after;
    errno = 0;
    result = wos_net_addr_set(&address);
    error = errno;
    check("addr-duplicate", result, error, ADMITTED ? EEXIST : EPERM, observe(address.ifindex, after) && same(current, after));
    wos_net_addr_req_v2 v2{};
    v2.size = sizeof(v2);
    v2.version = WOS_NETCTL_VERSION_1;
    v2.address = address;
    v2.address.replace = 1;
    v2.preferred_lifetime_s = UINT32_MAX;
    v2.valid_lifetime_s = UINT32_MAX;
    errno = 0;
    result = wos_net_addr_set_v2(&v2);
    error = errno;
    check("addr-v2-refresh", result, error, ADMITTED ? 0 : EPERM, observe(address.ifindex, after) && same(current, after));
    v2.valid_lifetime_s = 1;
    errno = 0;
    result = wos_net_addr_set_v2(&v2);
    error = errno;
    check("addr-v2-invalid", result, error, ADMITTED ? EINVAL : EPERM, observe(address.ifindex, after) && same(current, after));
    errno = 0;
    result = wos_net_route_set(&route);
    error = errno;
    expected = current;
    if (ADMITTED) expected.routes[expected.route_count++] = route;
    check("route-add", result, error, ADMITTED ? 0 : EPERM, observe(address.ifindex, after) && same(expected, after));
    current = after;
    auto invalid = route;
    invalid.reserved32 = 1;
    errno = 0;
    result = wos_net_route_set(&invalid);
    error = errno;
    check("route-invalid", result, error, ADMITTED ? EINVAL : EPERM, observe(address.ifindex, after) && same(current, after));
    errno = 0;
    result = wos_net_route_del(&route);
    error = errno;
    expected = current;
    if (ADMITTED) expected.routes[--expected.route_count] = {};
    check("route-del", result, error, ADMITTED ? 0 : EPERM, observe(address.ifindex, after) && same(expected, after));
    errno = 0;
    result = wos_net_addr_del(&address);
    error = errno;
    check("addr-del", result, error, ADMITTED ? 0 : EPERM, observe(address.ifindex, after) && same(baseline, after));
    errno = 0;
    result = wos_net_addr_del(&address);
    error = errno;
    check("addr-missing", result, error, ADMITTED ? EADDRNOTAVAIL : EPERM, observe(address.ifindex, after) && same(baseline, after));
    return failures || checks != 13 ? 92 : 0;
}
}  // namespace

auto main(int argc, char** argv) -> int {
    char node[64]{};
    if (argc != 3 || geteuid() != 0 || gethostname(node, sizeof(node)) || strcmp(node, argv[2])) return 2;
    alarm(240);
    wos_net_if_info interfaces[32]{};
    size_t count = 32;
    if (wos_net_if_list(interfaces, &count) || count > 32) return 3;
    unsigned index = 0;
    for (size_t i = 0; i < count; ++i)
        if (!strcmp(interfaces[i].name, "lo")) index = interfaces[i].ifindex;
    State baseline{};
    if (!index || !observe(index, baseline) || baseline.address_count == 16 || baseline.route_count == 128) return 4;
    wos_net_addr_req address{};
    address.ifindex = index;
    address.family = AF_INET6;
    address.prefix_len = 128;
    address.flags = WOS_NET_ADDR_F_PERMANENT | WOS_NET_ADDR_F_NOPREFIXROUTE | WOS_NET_ADDR_F_NODAD;
    if (inet_pton(AF_INET6, "2001:db8:19:9::1", address.local) != 1) return 5;
    wos_net_route_record route{};
    route.size = sizeof(route);
    route.version = WOS_NETCTL_VERSION_1;
    route.family = AF_INET6;
    route.ifindex = index;
    route.metric = 1919;
    route.prefix_len = 128;
    route.lifetime_s = UINT32_MAX;
    if (inet_pton(AF_INET6, "2001:db8:19:9::2", route.destination) != 1) return 5;
    for (size_t i = 0; i < baseline.address_count; ++i)
        if (!memcmp(baseline.addresses[i].local, address.local, 16)) return 6;
    for (size_t i = 0; i < baseline.route_count; ++i)
        if (!memcmp(baseline.routes[i].destination, route.destination, 16)) return 6;
    int const REPORT = open(argv[1], O_CREAT | O_EXCL | O_WRONLY, 0600);
    if (REPORT < 0) return 7;
    dprintf(REPORT, "node=%s parent=%ld loopback=%u\n", node, getpid(), index);
    unsigned failures = 0;
    for (unsigned mode = 0; mode < 6; ++mode) {
        pid_t const CHILD = fork();
        if (CHILD == 0) _exit(profile(mode, address, route, REPORT));
        int status = -1;
        if (CHILD < 0 || waitpid(CHILD, &status, 0) != CHILD || status) ++failures;
        int const REMOVE_ROUTE = wos_net_route_del(&route);
        if (REMOVE_ROUTE && errno != ENOENT) ++failures;
        int const REMOVE_ADDRESS = wos_net_addr_del(&address);
        if (REMOVE_ADDRESS && errno != EADDRNOTAVAIL) ++failures;
        State restored{};
        bool const RESTORED = observe(index, restored) && same(baseline, restored);
        if (!RESTORED) ++failures;
        dprintf(REPORT, "wait profile=%u pid=%ld status=%d restored=%d\n", mode, CHILD, status, RESTORED);
    }
    dprintf(REPORT, "%s: IPv6 authority profiles=6 checks=78 failures=%u\n", failures ? "FAIL" : "PASS", failures);
    int const SYNC = fsync(REPORT);
    close(REPORT);
    return failures || SYNC ? 1 : 0;
}
