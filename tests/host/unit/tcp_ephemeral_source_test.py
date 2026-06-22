#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TCP_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp.cpp"
TCP_INPUT_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "tcp_input.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def require(source: str, token: str, context: str) -> None:
    if token not in source:
        fail(f"{context}: missing {token}")


def main() -> None:
    source = TCP_CPP.read_text()
    input_source = TCP_INPUT_CPP.read_text()
    require(source, "constexpr uint16_t TCP_EPHEMERAL_PORT_FIRST = 49152", "ephemeral range")
    require(source, "constexpr uint16_t TCP_EPHEMERAL_PORT_LAST = 65535", "ephemeral range")
    require(source, "tcp_ephemeral_port_from_seed", "ephemeral seed mixing")
    require(source, "seed_tcp_ephemeral_port_once", "ephemeral lazy seed")
    require(source, "ker::mod::time::get_us()", "ephemeral time seed")
    require(source, "reinterpret_cast<uintptr_t>(&tcp_ephemeral_port)", "ephemeral address seed")
    require(source, "tcp_ephemeral_port == TCP_EPHEMERAL_PORT_LAST ? TCP_EPHEMERAL_PORT_FIRST", "ephemeral wrap")
    require(source, "auto tcp_generate_iss(uint32_t local_ip, uint16_t local_port, uint32_t remote_ip, uint16_t remote_port)", "ISS helper")
    require(source, "tcp_iss_tuple_hash(local_ip, local_port, remote_ip, remote_port)", "ISS tuple hash")
    require(source, "ker::mod::time::get_us() / 4U", "ISS time clock")
    require(source, "iss_counter.fetch_add(64000U", "ISS serial")
    require(source, "cb->iss = tcp_generate_iss(cb->local_ip, cb->local_port, cb->remote_ip, cb->remote_port)", "active ISS")
    if "auto alloc_ephemeral_port() -> uint16_t { return tcp_ephemeral_port++; }" in source:
        fail("ephemeral allocator must not restart deterministically at 49152")
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
