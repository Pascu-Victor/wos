#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
NETD_SRC_DIR = ROOT / "modules" / "netd" / "src"
NETD_INCLUDE_DIR = ROOT / "modules" / "netd" / "include"
ROOTFS_ALIASES = ROOT / "configs" / "rootfs" / "aliases.tsv"
ROOTFS_RESOLV_CONF = ROOT / "configs" / "rootfs" / "etc" / "resolv.conf"


def fail(message: str) -> None:
    raise AssertionError(message)


def read_netd_source() -> str:
    paths = [*sorted(NETD_SRC_DIR.glob("*.cpp")), *sorted(NETD_INCLUDE_DIR.rglob("*.hpp"))]
    return "\n".join(path.read_text() for path in paths)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|int|void|bool)\s+{re.escape(name)}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{",
        source,
    )
    if match is None:
        fail(f"missing function {name}")

    depth = 1
    pos = match.end()
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated function {name}")
    return source[match.end() : pos - 1]


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = found + len(token)


def switch_case(source: str, start_token: str, end_token: str, context: str) -> str:
    start = source.find(start_token)
    if start < 0:
        fail(f"{context}: missing switch case {start_token}")
    end = source.find(end_token, start + len(start_token))
    if end < 0:
        fail(f"{context}: missing following switch case {end_token}")
    return source[start:end]


def test_dhcp_lease_keeps_multiple_dns_servers() -> None:
    source = read_netd_source()
    if "constexpr size_t MAX_DNS_SERVERS = 3" not in source:
        fail("netd should retain a bounded standard resolv.conf nameserver set")
    if 'constexpr auto RESOLVER_OPTIONS = "options timeout:2 attempts:4\\n"' not in source:
        fail("netd should allow slow first-hop DNS while retaining a bounded retry budget")
    if "std::array<uint32_t, MAX_DNS_SERVERS> dns_servers" not in source:
        fail("DhcpLease must store multiple DNS servers")
    if "size_t dns_count" not in source:
        fail("DhcpLease must track DNS server count")


def test_dhcp_dns_option_parses_all_addresses() -> None:
    source = read_netd_source()
    helper = function_body(source, "remember_dns_server")
    require_order(
        helper,
        [
            "if (dns_host == 0)",
            "for (size_t i = 0; i < lease.dns_count; i++)",
            "if (lease.dns_servers[i] == dns_host)",
            "if (lease.dns_count >= lease.dns_servers.size())",
            "lease.dns_servers[lease.dns_count++] = dns_host",
            "if (lease.dns == 0)",
            "lease.dns = dns_host",
        ],
        "DNS server retention helper",
    )

    parse_reply = function_body(source, "parse_reply")
    opt_dns = switch_case(parse_reply, "case OPT_DNS:", "case OPT_DOMAIN_NAME:", "DHCP option 6")
    require_order(
        opt_dns,
        [
            "for (size_t offset = 0; offset + sizeof(uint32_t) <= OLEN; offset += sizeof(uint32_t))",
            "remember_dns_server(*lease, load_network_u32(opt + offset))",
        ],
        "DHCP option 6 parser",
    )


def test_resolv_conf_writes_every_dns_server() -> None:
    source = read_netd_source()
    writer = function_body(source, "write_resolv_conf")
    require_order(
        writer,
        [
            'fputs("# Managed by netd via DHCP\\n", file)',
            "fputs(RESOLVER_OPTIONS, file)",
            "if (lease.dns_count != 0)",
            "for (size_t i = 0; i < lease.dns_count; i++)",
            "ip_to_str(lease.dns_servers[i], dns_str.data(), dns_str.size())",
            'fprintf(file, "nameserver %s\\n", dns_str.data())',
            "} else if (lease.dns != 0)",
            "ip_to_str(lease.dns, dns_str.data(), dns_str.size())",
        ],
        "resolv.conf writer",
    )


def test_rootfs_seeds_resolv_conf_for_netd_rewrite() -> None:
    aliases = ROOTFS_ALIASES.read_text()
    resolv_conf = ROOTFS_RESOLV_CONF.read_text()
    if "copy\tconfigs/rootfs/etc/resolv.conf\t/etc/resolv.conf" not in aliases:
        fail("rootfs manifest must seed /etc/resolv.conf so netd can rewrite it without creating a new /etc entry")
    if '# Managed by netd via DHCP\n' not in resolv_conf:
        fail("seed resolv.conf should match netd ownership marker")
    if 'options timeout:2 attempts:4\n' not in resolv_conf:
        fail("seed resolv.conf should match netd resolver retry policy")


def test_renewal_compares_full_dns_set() -> None:
    source = read_netd_source()
    if "same_dns_servers(renew_lease, lease)" not in source:
        fail("DHCP renewal must compare the whole DNS server set")
    if "copy_dns_servers(lease, renew_lease)" not in source:
        fail("DHCP renewal must copy the whole DNS server set")
    if "copy_dns_servers(lease, ack_lease)" not in source:
        fail("DHCP ACK must copy the whole DNS server set")


def main() -> None:
    test_dhcp_lease_keeps_multiple_dns_servers()
    test_dhcp_dns_option_parses_all_addresses()
    test_resolv_conf_writes_every_dns_server()
    test_rootfs_seeds_resolv_conf_for_netd_rewrite()
    test_renewal_compares_full_dns_set()
    print("netd DNS source invariants hold")


if __name__ == "__main__":
    main()
