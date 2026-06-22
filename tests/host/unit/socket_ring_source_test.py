#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SOCKET_CPP = ROOT / "modules" / "kern" / "src" / "net" / "socket.cpp"
SOCKET_HPP = ROOT / "modules" / "kern" / "src" / "net" / "socket.hpp"
RAW_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "raw.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|int|void)\s+{re.escape(name)}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{",
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


def test_ring_buffer_publish_happens_after_full_copy() -> None:
    header = SOCKET_HPP.read_text()
    if "auto write_pair(const void* first_buf" not in header:
        fail("RingBuffer must expose write_pair for record-plus-payload socket writes")

    source = SOCKET_CPP.read_text()
    helper = function_body(source, "ring_write_unpublished")
    if "used." in helper:
        fail("ring_write_unpublished must not publish bytes before the caller finishes the complete write")
    require_order(
        helper,
        [
            "size_t const FIRST = std::min(len, ring->capacity - ring->write_pos)",
            "ker::util::copy_fast(ring->data + ring->write_pos, src, FIRST)",
            "size_t const SECOND = len - FIRST",
            "ker::util::copy_fast(ring->data, src + FIRST, SECOND)",
            "ring->write_pos = (ring->write_pos + len) % ring->capacity",
        ],
        "unpublished ring copy",
    )

    write = function_body(source, "RingBuffer::write")
    require_order(
        write,
        [
            "size_t const TO_WRITE = std::min(len, capacity - CUR)",
            "ring_write_unpublished(this, src, TO_WRITE)",
            "used.fetch_add(TO_WRITE, std::memory_order_release)",
        ],
        "single-buffer ring publish",
    )

    write_pair = function_body(source, "RingBuffer::write_pair")
    require_order(
        write_pair,
        [
            "size_t const TOTAL = first_len + second_len",
            "if (TOTAL > capacity - CUR)",
            "ring_write_unpublished(this, static_cast<const uint8_t*>(first_buf), first_len)",
            "ring_write_unpublished(this, static_cast<const uint8_t*>(second_buf), second_len)",
            "used.fetch_add(TOTAL, std::memory_order_release)",
        ],
        "pair ring publish",
    )
    if write_pair.count("used.fetch_add") != 1:
        fail("RingBuffer::write_pair must publish record+payload with one release increment")


def test_raw_socket_packets_keep_complete_receive_records() -> None:
    source = RAW_CPP.read_text()
    if "struct RawRecvRecord" not in source:
        fail("raw sockets must keep a fixed receive record before each packet")
    if "static_assert(sizeof(RawRecvRecord) == 8)" not in source:
        fail("raw receive record size must stay fixed")

    recvfrom = function_body(source, "raw_recvfrom")
    require_order(
        recvfrom,
        [
            "sock->rcvbuf.available() < sizeof(RawRecvRecord)",
            "sock->rcvbuf.read(&record, sizeof(record))",
            "size_t const PACKET_LEN = record.packet_len",
            "sock->rcvbuf.available() < PACKET_LEN",
            "size_t const TO_COPY = len < PACKET_LEN ? len : PACKET_LEN",
            "sock->rcvbuf.read(buf, TO_COPY)",
            "size_t remaining = PACKET_LEN - TO_COPY",
            "sock->rcvbuf.read(discard.data(), CHUNK)",
            "socket_fill_sockaddr_v4(addr_out, *addr_len, nullptr, record.src_ip, 0)",
        ],
        "raw recvfrom complete packet consumption",
    )

    deliver = function_body(source, "raw_deliver")
    require_order(
        deliver,
        [
            "constexpr size_t PREPEND_LEN = sizeof(RawRecvRecord) + sizeof(IPv4Header)",
            "auto* frame = pkt->push(PREPEND_LEN)",
            "record->packet_len = static_cast<uint16_t>(sizeof(IPv4Header) + PAYLOAD_LEN)",
            "if (sock->rcvbuf.available() + pkt->len > sock->rcvbuf.capacity)",
            "sock->rcvbuf.write(pkt->data, pkt->len)",
        ],
        "raw deliver complete packet enqueue",
    )


def main() -> None:
    test_ring_buffer_publish_happens_after_full_copy()
    test_raw_socket_packets_keep_complete_receive_records()
    print("socket ring and raw packet receive source invariants hold")


if __name__ == "__main__":
    main()
