#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "modules" / "init" / "src" / "network.cpp"


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


def main() -> None:
    source = SOURCE.read_text()
    for token in (
        "constexpr size_t IF_DEBUG_CAP = 16",
        "constexpr size_t IPV4_ADDRS_PER_INTERFACE_CAP = 8",
        "constexpr size_t IPV6_ADDRS_PER_INTERFACE_CAP = 8",
        "IF_DEBUG_CAP * (IPV4_ADDRS_PER_INTERFACE_CAP + IPV6_ADDRS_PER_INTERFACE_CAP)",
        "static_assert(ADDR_DEBUG_CAP == 256)",
    ):
        if token not in source:
            raise AssertionError(f"init NETCTL address bound missing {token}")

    readiness = function_body(source, "interface_has_usable_global_ipv6")
    for token in (
        "std::array<wos_net_addr_info, ADDR_DEBUG_CAP>",
        "std::min(address_count, addresses.size())",
        "WOS_NET_ADDR_F_TENTATIVE | WOS_NET_ADDR_F_DADFAILED",
    ):
        if token not in readiness:
            raise AssertionError(f"IPv6 readiness scan missing bounded/full-capacity behavior: {token}")

    dump = function_body(source, "dump_netctl_state")
    for token in (
        "std::array<wos_net_addr_info, ADDR_DEBUG_CAP>",
        "std::min(addr_count, addrs.size())",
    ):
        if token not in dump:
            raise AssertionError(f"NETCTL diagnostics must bound a truncated kernel count: {token}")
    print("init IPv6 readiness scans the full bounded ABI address surface")


if __name__ == "__main__":
    main()
