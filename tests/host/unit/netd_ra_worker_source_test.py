#!/usr/bin/env python3

import re
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
    print("netd RA worker source invariants hold")


if __name__ == "__main__":
    main()
