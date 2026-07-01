#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
ROUTING_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "routing.cpp"
ROUTING_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "routing.hpp"
WKI_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\b(?:void|auto)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{", source)
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


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = found + len(token)


def test_routing_lookup_returns_locked_snapshot() -> None:
    header = ROUTING_HPP.read_text()
    source = ROUTING_CPP.read_text()

    require_tokens(
        header,
        ["auto wki_routing_lookup(uint16_t dst_node, RoutingEntry* out) -> bool;"],
        "routing snapshot declaration",
    )
    if "auto wki_routing_lookup(uint16_t dst_node) -> const RoutingEntry*" in header + source:
        fail("routing lookup must not return a pointer into the mutable routing table")

    body = function_body(source, "wki_routing_lookup")
    require_order(
        body,
        [
            "if (out == nullptr)",
            "return false",
            "s_routing_lock.lock()",
            "for (auto const& route : s_routing_table)",
            "if (route.valid && route.dst_node == dst_node)",
            "*out = route",
            "s_routing_lock.unlock()",
            "return true",
            "s_routing_lock.unlock()",
            "return false",
        ],
        "routing lookup snapshot locking",
    )
    if "return &route" in body:
        fail("routing lookup must copy route snapshots rather than return route addresses")


def test_wki_send_paths_consume_route_snapshots() -> None:
    source = WKI_CPP.read_text()
    find_transport_body = function_body(source, "find_transport_for_peer")
    resolve_next_hop_body = function_body(source, "resolve_next_hop")

    for name, body in (("find_transport_for_peer", find_transport_body), ("resolve_next_hop", resolve_next_hop_body)):
        if "const RoutingEntry* route = wki_routing_lookup" in body or "route->" in body:
            fail(f"{name} must not retain a pointer returned by routing lookup")

    require_order(
        find_transport_body,
        [
            "RoutingEntry route = {}",
            "wki_routing_lookup(dst_node, &route)",
            "route.valid && route.next_hop != WKI_NODE_INVALID",
            "wki_peer_find(route.next_hop)",
        ],
        "transport route snapshot",
    )
    require_order(
        resolve_next_hop_body,
        [
            "RoutingEntry route = {}",
            "wki_routing_lookup(dst_node, &route)",
            "route.valid",
            "return route.next_hop",
        ],
        "next-hop route snapshot",
    )


def main() -> None:
    test_routing_lookup_returns_locked_snapshot()
    test_wki_send_paths_consume_route_snapshots()
    print("WKI routing source invariants hold")


if __name__ == "__main__":
    main()
