#!/usr/bin/env python3

import re
import shutil
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "modules" / "netd" / "src" / "ra_worker.cpp"
MANAGER_SOURCE = ROOT / "modules" / "netd" / "src" / "manager.cpp"


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\b(?:void|auto)\s+{re.escape(name)}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{", source)
    if match is None:
        raise AssertionError(f"missing function {name}")
    depth = 1
    cursor = match.end()
    while cursor < len(source) and depth:
        depth += source[cursor] == "{"
        depth -= source[cursor] == "}"
        cursor += 1
    if depth:
        raise AssertionError(f"unterminated function {name}")
    return source[match.end() : cursor - 1]


def require_order(source: str, tokens: tuple[str, ...], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            raise AssertionError(f"{context}: missing ordered token {token}")
        cursor = found + len(token)


def exercise_readiness_wait(source: str) -> None:
    # The workspace PATH can put the WOS cross compiler ahead of host tools.
    compiler = shutil.which("c++", path="/usr/bin:/bin")
    if compiler is None:
        raise RuntimeError("native C++ compiler required for RA readiness test")
    helper = "auto wait_for_ra_packet(int socket_fd, uint64_t deadline_us) -> int {" + function_body(source, "wait_for_ra_packet") + "}"
    program = r'''
#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <limits>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <thread>
auto monotonic_now_us() -> uint64_t {
    timespec now{}; assert(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
    return uint64_t(now.tv_sec) * 1000000 + uint64_t(now.tv_nsec) / 1000;
}
int poll_calls = 0;
auto counted_poll(pollfd* fds, nfds_t count, int timeout) -> int {
    ++poll_calls; return ::poll(fds, count, timeout);
}
#define poll counted_poll
''' + helper + r'''
#undef poll
volatile sig_atomic_t interrupted = 0;
void signal_handler(int) { interrupted = 1; }
int main() {
    int sockets[2]; assert(socketpair(AF_UNIX, SOCK_DGRAM, 0, sockets) == 0);
    uint64_t start = monotonic_now_us();
    assert(wait_for_ra_packet(sockets[0], start + 70000) == 0);
    assert(monotonic_now_us() >= start + 70000 && poll_calls == 1);
    assert(send(sockets[1], "a", 1, 0) == 1);
    assert(wait_for_ra_packet(sockets[0], monotonic_now_us() + 2000000) == 1);
    char byte; assert(recv(sockets[0], &byte, 1, 0) == 1 && byte == 'a');
    std::thread writer([&] { usleep(20000); assert(send(sockets[1], "b", 1, 0) == 1); });
    start = monotonic_now_us();
    assert(wait_for_ra_packet(sockets[0], start + 2000000) == 1);
    assert(monotonic_now_us() < start + 1500000);
    assert(recv(sockets[0], &byte, 1, 0) == 1 && byte == 'b'); writer.join();
    struct sigaction action{}; action.sa_handler = signal_handler;
    assert(sigemptyset(&action.sa_mask) == 0 && sigaction(SIGALRM, &action, nullptr) == 0);
    itimerval timer{}; timer.it_value.tv_usec = 10000; timer.it_interval.tv_usec = 10000;
    assert(setitimer(ITIMER_REAL, &timer, nullptr) == 0);
    start = monotonic_now_us();
    assert(wait_for_ra_packet(sockets[0], start + 70000) == 0);
    uint64_t elapsed = monotonic_now_us() - start;
    timer = {}; assert(setitimer(ITIMER_REAL, &timer, nullptr) == 0);
    assert(interrupted && elapsed >= 70000 && elapsed < 500000);
    close(sockets[0]);
    assert(wait_for_ra_packet(sockets[0], monotonic_now_us() + 2000000) == -1 && errno == EBADF);
    poll_calls = 0;
    assert(wait_for_ra_packet(sockets[1], monotonic_now_us()) == 0 && poll_calls == 0);
    close(sockets[1]);
}
'''
    with tempfile.TemporaryDirectory(prefix="wos-ra-readiness-") as temporary:
        cpp = Path(temporary) / "readiness.cpp"
        binary = Path(temporary) / "readiness"
        cpp.write_text(program)
        subprocess.run([compiler, "-std=c++20", "-O2", "-pthread", str(cpp), "-o", str(binary)], check=True)
        subprocess.run([str(binary)], check=True, timeout=5)


def main() -> None:
    source = SOURCE.read_text()
    identity = function_body(source, "find_interface_identity")
    if "ipv6_link_local_usable(candidate, address.scope, address.flags)" not in identity:
        raise AssertionError("RA socket identity must reject tentative, DAD-failed, or wrongly scoped link-local rows")

    receive = function_body(source, "receive_router_advertisements")
    require_order(
        receive,
        (
            "next_identity_check_us",
            "find_interface_identity(ifname, current)",
            "same_interface_identity(identity, current)",
            "current.ifindex == identity.ifindex && current.mac == identity.mac",
            "withdraw_ra_state(ifname, identity, state, SAME_DEVICE)",
            "return",
        ),
        "bounded RA identity retirement",
    )
    require_order(
        receive,
        ("apply_ra_plan(ifname, identity.ifindex, PLAN, state)", "if (MTU_APPLIED)", "identity.mtu = PLAN.mtu"),
        "RA MTU cache updates only after successful NETCTL application",
    )

    withdraw = function_body(source, "withdraw_ra_state")
    for token in (
        "device_still_matches && state.route_installed",
        "device_still_matches && state.onlink_route_installed",
        "device_still_matches && state.address_installed",
        "state = {}",
    ):
        if token not in withdraw:
            raise AssertionError(f"stale RA state retirement missing {token}")

    manager = MANAGER_SOURCE.read_text()
    run_daemon = function_body(manager, "run_network_daemon")
    require_order(
        run_daemon,
        (
            "start_ra_worker(worker_count, interface.ifname.data())",
            "uint32_t retry_delay_s = DHCP_RETRY_INITIAL_S",
            "run_dhcp_client(dhcp_ifname)",
            "IPv6 workers remain active",
            "sleep_for_seconds(retry_delay_s)",
            "std::min(DHCP_RETRY_MAX_S, retry_delay_s * 2U)",
        ),
        "DHCP failure must not terminate detached IPv6 workers",
    )
    if run_daemon.count("start_ra_worker(") != 1:
        raise AssertionError("DHCP retries must not create additional RA worker threads")
    if "return run_dhcp_client" in run_daemon:
        raise AssertionError("run_network_daemon must not exit when DHCP acquisition fails")
    if "RA_POLL_INTERVAL_US" in source or "wait_for_ra_packet(socket_fd, DEADLINE_US)" not in receive:
        raise AssertionError("idle RA reception must block on packet readiness and absolute control deadlines")
    exercise_readiness_wait(source)
    print("netd RA worker source invariants and native readiness tests hold")


if __name__ == "__main__":
    main()
