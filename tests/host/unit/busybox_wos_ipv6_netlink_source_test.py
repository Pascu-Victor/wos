#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "toolchain" / "src" / "busybox" / "networking" / "libiproute" / "libnetlink.c"


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\b(?:static\s+)?(?:int|void)\s+{re.escape(name)}\([^{{]*\)\s*\{{", source)
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
    emit = function_body(source, "wos_emit_addr")
    for token in (
        "info->family == AF_INET6",
        "addr_len = 16",
        "IFA_ADDRESS, (void *)info->address, addr_len",
        "IFA_LOCAL, (void *)info->local, addr_len",
        "if (info->family == AF_INET)",
    ):
        if token not in emit:
            raise AssertionError(f"WOS BusyBox IPv6 address emission missing {token}")

    apply = function_body(source, "wos_apply_addr_msg")
    for token in (
        "ifa->ifa_family == AF_INET6",
        "RTA_PAYLOAD(tb[IFA_ADDRESS]) != addr_len",
        "RTA_PAYLOAD(tb[IFA_LOCAL]) != addr_len",
        "memcpy(req.address, RTA_DATA(tb[IFA_ADDRESS]), addr_len)",
        "memcpy(req.local, RTA_DATA(tb[IFA_LOCAL]), addr_len)",
    ):
        if token not in apply:
            raise AssertionError(f"WOS BusyBox IPv6 address application missing {token}")
    print("BusyBox WOS IPv6 netlink source invariants hold")


if __name__ == "__main__":
    main()
