#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
LOOKUP_CPP = ROOT / "toolchain" / "src" / "mlibc" / "options" / "posix" / "generic" / "lookup.cpp"
RESOLV_CONF_CPP = ROOT / "toolchain" / "src" / "mlibc" / "options" / "posix" / "generic" / "resolv_conf.cpp"
RESOLV_CONF_HPP = ROOT / "toolchain" / "src" / "mlibc" / "options" / "posix" / "include" / "mlibc" / "resolv_conf.hpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b{re.escape(name)}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{",
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


def test_dns_uses_standard_udp_response_size() -> None:
    source = LOOKUP_CPP.read_text()
    if "constexpr size_t DNS_UDP_RESPONSE_SIZE = 512" not in source:
        fail("DNS UDP response buffer must use the standard 512 byte payload size")
    if "constexpr int DNS_QUERY_ATTEMPTS = 2" not in source:
        fail("DNS lookups should retry once after retryable resolver failures")
    if "constexpr int DNS_QUERY_TIMEOUT_MS = 5000" not in source:
        fail("DNS lookups should keep the standard 5 second default timeout")
    if "constexpr uint16_t DNS_FLAG_RESPONSE = 0x8000" not in source:
        fail("DNS lookups must require the QR=response flag")
    if "char response[256]" in source or "recvfrom(fd, response, 256" in source:
        fail("DNS lookups must not truncate UDP responses at 256 bytes")
    if source.count("char response[DNS_UDP_RESPONSE_SIZE]") != 2:
        fail("both forward and reverse DNS lookups should use DNS_UDP_RESPONSE_SIZE")


def test_dotted_names_query_absolute_before_search() -> None:
    source = LOOKUP_CPP.read_text()
    lookup_name_dns = function_body(source, "lookup_name_dns")
    require_order(
        lookup_name_dns,
        [
            "bool const try_absolute_first = !has_trailing_dot && strchr(name, '.') != nullptr",
            "if (try_absolute_first)",
            "lookup_name_dns_one(buf, name, canon_name, family)",
            "if (count > 0)",
            "return count",
            "if (count < 0 && count != -EAI_NONAME)",
            "return count",
            "if (!has_trailing_dot && conf && !conf->search.empty())",
            "qualified += domain",
        ],
        "dotted-name DNS search order",
    )


def test_resolv_conf_keeps_all_nameservers() -> None:
    header = RESOLV_CONF_HPP.read_text()
    if "frg::vector<frg::string<MemoryAllocator>, MemoryAllocator> nameservers" not in header:
        fail("resolv_conf_data must retain every nameserver line")
    if "timeout(5)" not in header or "attempts(2)" not in header:
        fail("resolv_conf_data must retain standard resolver timing defaults")
    if "int timeout" not in header or "int attempts" not in header:
        fail("resolv_conf_data must expose parsed resolver timing options")

    source = RESOLV_CONF_CPP.read_text()
    parser = function_body(source, "get_resolv_conf")
    require_order(
        parser,
        [
            'if (!strcmp(key, "nameserver"))',
            "frg::string<MemoryAllocator> nameserver",
            "if (ret.name.empty())",
            "ret.name = nameserver",
            "ret.nameservers.push(std::move(nameserver))",
        ],
        "resolv.conf nameserver parsing",
    )

    get_nameserver = function_body(source, "get_nameserver")
    require_order(
        get_nameserver,
        [
            "conf->nameservers.empty()",
            "if (!conf->nameservers.empty())",
            "ret.name = std::move(conf->nameservers[0])",
        ],
        "legacy first nameserver accessor",
    )


def test_resolv_conf_parses_timeout_options() -> None:
    source = RESOLV_CONF_CPP.read_text()
    if "MIN_RESOLV_TIMEOUT_SECS = 1" not in source:
        fail("resolver timeout should be clamped to a sane minimum")
    if "MAX_RESOLV_TIMEOUT_SECS = 30" not in source:
        fail("resolver timeout should be clamped to a sane maximum")
    if "MIN_RESOLV_ATTEMPTS = 1" not in source:
        fail("resolver attempts should be clamped to a sane minimum")
    if "MAX_RESOLV_ATTEMPTS = 5" not in source:
        fail("resolver attempts should be clamped to a sane maximum")

    apply_option = function_body(source, "apply_resolv_option")
    require_order(
        apply_option,
        [
            'TIMEOUT_PREFIX[] = "timeout:"',
            'ATTEMPTS_PREFIX[] = "attempts:"',
            "parse_positive_option_value(token + sizeof(TIMEOUT_PREFIX) - 1, &value)",
            "ret.timeout = clamp_resolv_option(value, MIN_RESOLV_TIMEOUT_SECS, MAX_RESOLV_TIMEOUT_SECS)",
            "parse_positive_option_value(token + sizeof(ATTEMPTS_PREFIX) - 1, &value)",
            "ret.attempts = clamp_resolv_option(value, MIN_RESOLV_ATTEMPTS, MAX_RESOLV_ATTEMPTS)",
        ],
        "resolv.conf option application",
    )

    parser = function_body(source, "get_resolv_conf")
    require_order(
        parser,
        [
            'else if (!strcmp(key, "options"))',
            "while (*pos)",
            "char saved = *end",
            "*end = '\\0'",
            "apply_resolv_option(ret, pos)",
            "pos = end + 1",
        ],
        "resolv.conf options parsing",
    )


def test_dns_tries_multiple_nameservers() -> None:
    source = LOOKUP_CPP.read_text()
    forward = function_body(source, "lookup_name_dns_one")
    require_order(
        forward,
        [
            "auto conf = get_resolv_conf()",
            "int const attempts = conf ? conf->attempts : DNS_QUERY_ATTEMPTS",
            "int const timeout_ms = conf ? conf->timeout * MSEC_PER_SEC : DNS_QUERY_TIMEOUT_MS",
            "conf->nameservers.empty()",
            "for (auto &nameserver : conf->nameservers)",
            "lookup_name_dns_one_at_with_retries(",
            "buf, name, canon_name, family, nameserver.data(), attempts, timeout_ms",
            "if (count > 0)",
            "return count",
            "if (count == 0 || count == -EAI_NONAME || fallback == -EAI_AGAIN)",
            "fallback = count",
        ],
        "forward DNS nameserver fallback",
    )

    reverse = function_body(source, "lookup_addr_dns")
    require_order(
        reverse,
        [
            "auto conf = get_resolv_conf()",
            "int const attempts = conf ? conf->attempts : DNS_QUERY_ATTEMPTS",
            "int const timeout_ms = conf ? conf->timeout * MSEC_PER_SEC : DNS_QUERY_TIMEOUT_MS",
            "conf->nameservers.empty()",
            "for (auto &nameserver : conf->nameservers)",
            "lookup_addr_dns_at_with_retries(",
            "name, addr, family, nameserver.data(), attempts, timeout_ms",
            "if (count > 0)",
            "return count",
            "if (count == 0 || count == -EAI_NONAME || fallback == -EAI_AGAIN)",
            "fallback = count",
        ],
        "reverse DNS nameserver fallback",
    )


def test_dns_retries_retryable_failures() -> None:
    source = LOOKUP_CPP.read_text()
    forward_retry = function_body(source, "lookup_name_dns_one_at_with_retries")
    require_order(
        forward_retry,
        [
            "for (int attempt = 0; attempt < attempts; attempt++)",
            "int fd = socket(AF_INET, SOCK_DGRAM, 0)",
            "frg::scope_exit close_fd",
            "lookup_name_dns_one_at(buf, name, canon_name, family, nameserver_name, fd, timeout_ms)",
            "if (count != -EAI_AGAIN)",
            "return count",
            "fallback = count",
        ],
        "forward DNS retry wrapper",
    )

    reverse_retry = function_body(source, "lookup_addr_dns_at_with_retries")
    require_order(
        reverse_retry,
        [
            "for (int attempt = 0; attempt < attempts; attempt++)",
            "int fd = socket(AF_INET, SOCK_DGRAM, 0)",
            "frg::scope_exit close_fd",
            "lookup_addr_dns_at(name, addr, family, nameserver_name, fd, timeout_ms)",
            "if (count != -EAI_AGAIN)",
            "return count",
            "fallback = count",
        ],
        "reverse DNS retry wrapper",
    )


def test_dns_retries_use_fresh_query_identity() -> None:
    source = LOOKUP_CPP.read_text()
    next_id = function_body(source, "next_dns_query_id")
    if "__atomic_add_fetch(&next_id, 1, __ATOMIC_RELAXED)" not in next_id:
        fail("DNS transaction IDs should advance atomically across lookups")

    if source.count("header.identification = htons(next_dns_query_id())") != 2:
        fail("forward and reverse DNS queries must use fresh transaction IDs")

    forward_retry = function_body(source, "lookup_name_dns_one_at_with_retries")
    reverse_retry = function_body(source, "lookup_addr_dns_at_with_retries")
    for body, context in (
        (forward_retry, "forward DNS retry wrapper"),
        (reverse_retry, "reverse DNS retry wrapper"),
    ):
        loop_pos = body.find("for (int attempt = 0; attempt < attempts; attempt++)")
        socket_pos = body.find("int fd = socket(AF_INET, SOCK_DGRAM, 0)")
        close_pos = body.find("frg::scope_exit close_fd")
        if loop_pos < 0 or socket_pos < loop_pos or close_pos < socket_pos:
            fail(f"{context}: each retry must open and close its own UDP socket")


def test_dns_answer_parser_advances_rdata_once() -> None:
    source = LOOKUP_CPP.read_text()
    forward = function_body(source, "lookup_name_dns_one_at")
    require_order(
        forward,
        [
            "struct sockaddr_in peer",
            "recvfrom(",
            "reinterpret_cast<struct sockaddr *>(&peer)",
            "peer.sin_port != sin.sin_port",
            "peer.sin_addr.s_addr != sin.sin_addr.s_addr",
            "continue",
            "if (response_header->identification != header.identification)",
            "continue",
            "uint16_t const flags = ntohs(response_header->flags)",
            "if ((flags & DNS_FLAG_RESPONSE) == 0)",
            "continue",
            "uint16_t const rcode = flags & 0xF",
            "if (rcode == RETURN_NXDOMAIN)",
            "return -EAI_NONAME",
            "if (rcode != RETURN_NOERROR)",
            "return -EAI_AGAIN",
            "uint16_t const answer_count = ntohs(response_header->no_ans)",
            "if (answer_count == 0)",
            "return 0",
            "char *rr_data = it",
            "it += rr_length",
            "if (rr_length != 4)",
            "memcpy(buffer.addr, rr_data, rr_length)",
            "if (rr_length != 16)",
            "memcpy(buffer.addr, rr_data, rr_length)",
            "canon_name = read_dns_name(response, rr_data)",
        ],
        "forward DNS answer parsing",
    )

    reverse = function_body(source, "lookup_addr_dns_at")
    require_order(
        reverse,
        [
            "struct sockaddr_in peer",
            "recvfrom(",
            "reinterpret_cast<struct sockaddr *>(&peer)",
            "peer.sin_port != sin.sin_port",
            "peer.sin_addr.s_addr != sin.sin_addr.s_addr",
            "continue",
            "if (response_header->identification != header.identification)",
            "continue",
            "uint16_t const flags = ntohs(response_header->flags)",
            "if ((flags & DNS_FLAG_RESPONSE) == 0)",
            "continue",
            "uint16_t const rcode = flags & 0xF",
            "if (rcode == RETURN_NXDOMAIN)",
            "return -EAI_NONAME",
            "if (rcode != RETURN_NOERROR)",
            "return -EAI_AGAIN",
            "uint16_t const answer_count = ntohs(response_header->no_ans)",
            "if (answer_count == 0)",
            "return 0",
            "char *rr_data = it",
            "it += rr_length",
            "auto ptr_name = read_dns_name(response, rr_data)",
        ],
        "reverse DNS answer parsing",
    )


def main() -> None:
    test_dns_uses_standard_udp_response_size()
    test_dotted_names_query_absolute_before_search()
    test_resolv_conf_keeps_all_nameservers()
    test_resolv_conf_parses_timeout_options()
    test_dns_tries_multiple_nameservers()
    test_dns_retries_retryable_failures()
    test_dns_retries_use_fresh_query_identity()
    test_dns_answer_parser_advances_rdata_once()
    print("mlibc DNS resolver source invariants hold")


if __name__ == "__main__":
    main()
