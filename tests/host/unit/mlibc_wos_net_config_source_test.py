#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "toolchain" / "src" / "mlibc" / "sysdeps" / "wos" / "generic" / "net_config.cpp"


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\b(?:bool|void|int)\s+{re.escape(name)}\([^)]*\)\s*\{{", source)
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
    ipv6 = function_body(source, "fill_ipv6")
    if "fill_prefix(reinterpret_cast<uint8_t *>(&netmask->sin6_addr), 16, info.prefix_len)" not in ipv6:
        raise AssertionError("WOS getifaddrs must construct the IPv6 netmask without a nullable-address detour")

    configured = function_body(source, "usable_global_ipv6")
    for token in (
        "address.family == AF_INET6",
        "address.scope == 0",
        "WOS_NET_ADDR_F_TENTATIVE",
        "WOS_NET_ADDR_F_DADFAILED",
    ):
        if token not in configured:
            raise AssertionError(f"WOS AI_ADDRCONFIG IPv6 filter missing {token}")
    if "usable_global_ipv6(addresses[i])" not in source:
        raise AssertionError("InetConfigured must use the validated global IPv6 readiness filter")
    print("mlibc WOS address visibility source invariants hold")


if __name__ == "__main__":
    main()
