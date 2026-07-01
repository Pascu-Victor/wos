#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TCP_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp.cpp"
TCP_INPUT_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp_input.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def require(source: str, token: str, context: str) -> None:
    if token not in source:
        fail(f"{context}: missing {token}")


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\b(?:int|void|auto)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{", source)
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


def main() -> None:
    source = TCP_CPP.read_text()
    input_source = TCP_INPUT_CPP.read_text()
    reserve_body = function_body(source, "reserve_ephemeral_port_for_connect")
    connect_body = function_body(source, "tcp_connect")
    require(source, "constexpr uint16_t TCP_EPHEMERAL_PORT_FIRST = 49152", "ephemeral range")
    require(source, "constexpr uint16_t TCP_EPHEMERAL_PORT_LAST = 65535", "ephemeral range")
    require(source, "tcp_ephemeral_port_from_seed", "ephemeral seed mixing")
    require(source, "seed_tcp_ephemeral_port_once", "ephemeral lazy seed")
    require(source, "ker::mod::time::get_us()", "ephemeral time seed")
    require(source, "reinterpret_cast<uintptr_t>(&tcp_ephemeral_port)", "ephemeral address seed")
    require(source, "tcp_next_ephemeral_port", "ephemeral wrap helper")
    require(source, "auto tcp_generate_iss(uint32_t local_ip, uint16_t local_port, uint32_t remote_ip, uint16_t remote_port)", "ISS helper")
    require(source, "tcp_iss_tuple_hash(local_ip, local_port, remote_ip, remote_port)", "ISS tuple hash")
    require(source, "ker::mod::time::get_us() / 4U", "ISS time clock")
    require(source, "iss_counter.fetch_add(64000U", "ISS serial")
    require(source, "cb->iss = tcp_generate_iss(cb->local_ip, cb->local_port, cb->remote_ip, cb->remote_port)", "active ISS")
    require_order(
        reserve_body,
        [
            "tcp_bind_lock.lock()",
            "seed_tcp_ephemeral_port_once()",
            "TcpBinding* slot = tcp_free_binding_slot_locked()",
            "if (slot == nullptr)",
            "return -EADDRNOTAVAIL",
            "for (uint32_t attempts = 0; attempts < TCP_EPHEMERAL_PORT_COUNT; attempts++)",
            "uint16_t const PORT = tcp_ephemeral_port",
            "tcp_ephemeral_port = tcp_next_ephemeral_port(tcp_ephemeral_port)",
            "if (tcp_binding_conflicts_locked(local_ip, PORT))",
            "continue",
            "slot->cb = cb",
            "slot->local_ip = local_ip",
            "slot->local_port = PORT",
            "cb->local_port = PORT",
            "sock->local_v4.port = PORT",
            "tcp_bind_lock.unlock()",
            "return 0",
            "tcp_bind_lock.unlock()",
            "return -EADDRNOTAVAIL",
        ],
        "active connect ephemeral reservation",
    )
    require_order(
        connect_body,
        [
            "if (cb->local_ip == 0)",
            "ker::net::route_lookup(ip)",
            "if (cb->local_port == 0)",
            "int const BIND_RET = reserve_ephemeral_port_for_connect(sock, cb, cb->local_ip)",
            "if (BIND_RET != 0)",
            "return BIND_RET",
            "cb->remote_ip = ip",
            "tcp_insert_cb(cb)",
        ],
        "tcp_connect resolves local IP before reserving ephemeral port",
    )
    if "auto alloc_ephemeral_port() -> uint16_t" in source or "cb->local_port = alloc_ephemeral_port()" in source:
        fail("active-open ephemeral allocation must reserve under tcp_bind_lock")
    if "iss_counter ^= (iss_counter << 13)" in source:
        fail("TCP ISS must not use the old boot-fixed xorshift stream")
    require(
        input_source,
        "child_cb->iss = tcp_generate_iss(child_cb->local_ip, child_cb->local_port, child_cb->remote_ip, child_cb->remote_port)",
        "passive ISS",
    )
    if "0xDEADBEEF" in input_source:
        fail("passive TCP ISS must not derive from the peer sequence with a fixed constant")
    print("TCP ephemeral port source invariants hold")


if __name__ == "__main__":
    main()
