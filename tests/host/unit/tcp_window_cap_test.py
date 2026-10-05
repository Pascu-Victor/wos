#!/usr/bin/env python3
"""Verify bounded TCP window rewriting with independent full checksum validation."""

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


bridge = module(ROOT / "scripts/bench/tcp_impairment_bridge.py", "bridge")
cap = bridge


def checksum(data):
    data += bytes(len(data) % 2)
    value = sum(int.from_bytes(data[i:i + 2], "big") for i in range(0, len(data), 2))
    while value >> 16:
        value = (value & 0xffff) + (value >> 16)
    return ~value & 0xffff


def tcp_bytes(frame):
    version = frame[14] >> 4
    if version == 4:
        offset = 14 + (frame[14] & 15) * 4
        end = 14 + int.from_bytes(frame[16:18], "big")
        pseudo = frame[26:34] + bytes([0, 6]) + (end - offset).to_bytes(2, "big")
    else:
        offset = 54
        end = 54 + int.from_bytes(frame[18:20], "big")
        pseudo = frame[22:54] + (end - offset).to_bytes(4, "big") + bytes([0, 0, 0, 6])
    return offset, pseudo + frame[offset:end]


def packet(version, reverse=False, flags=0x10, scale=None, window=65535, port=50000, payload=b"", raw_options=None):
    options = bytes([3, 3, scale, 1]) if scale is not None else b""
    if raw_options is not None:
        options = bytes(raw_options)
        options += bytes(-len(options) % 4)
    size = 20 + len(options) + len(payload)
    frame = bytearray(14 + (20 if version == 4 else 40) + size)
    frame[12:14] = (0x800 if version == 4 else 0x86dd).to_bytes(2, "big")
    if version == 4:
        frame[14] = 0x45; frame[23] = 6; frame[16:18] = (20 + size).to_bytes(2, "big")
        frame[26:34] = bytes([10, 29, 0, 2, 10, 29, 0, 1]) if not reverse else bytes([10, 29, 0, 1, 10, 29, 0, 2])
        offset = 34
    else:
        frame[14] = 0x60; frame[20] = 6; frame[18:20] = size.to_bytes(2, "big")
        left, right = bytes([0xfe, 0x80]) + bytes(13) + b"\x02", bytes([0xfe, 0x80]) + bytes(13) + b"\x01"
        frame[22:54] = left + right if not reverse else right + left
        offset = 54
    frame[offset:offset + 4] = (port.to_bytes(2, "big") + (49000 + version).to_bytes(2, "big")) if not reverse else ((49000 + version).to_bytes(2, "big") + port.to_bytes(2, "big"))
    frame[offset + 12] = (5 + len(options) // 4) << 4; frame[offset + 13] = flags
    frame[offset + 14:offset + 16] = window.to_bytes(2, "big")
    frame[offset + 20:] = options + payload
    _, data = tcp_bytes(bytes(frame)); frame[offset + 16:offset + 18] = checksum(data).to_bytes(2, "big")
    return bytes(frame)


assert cap.checksum_replace16(0xdd2f, 0x5555, 0x3285) == 0
for version in (4, 6):
    for client, server in ((0, 0), (14, 14), (4, 4), (None, None), (None, 14), (14, None)):
        rewrite = cap.WindowCap(131072)
        for reverse, scale, flags in ((False, client, 2), (True, server, 0x12)):
            syn = packet(version, reverse, flags, scale)
            assert rewrite.rewrite(syn) == syn
        for reverse, own_scale in ((False, client), (True, server)):
            scale = own_scale if client is not None and server is not None else 0
            for window in (0, 1, 123, 65535):
                original = packet(version, reverse, window=window, payload=b"odd payload")
                changed = rewrite.rewrite(original); offset, data = tcp_bytes(changed)
                assert checksum(data) == 0
                expected = min(window, min(65535, 131072 >> scale))
                assert int.from_bytes(changed[offset + 14:offset + 16], "big") == expected
                assert changed[:offset] == original[:offset] and changed[offset + 18:] == original[offset + 18:]
                assert expected << scale <= 131072
        assert rewrite.counts[f"ipv{version}_maximum_effective_window"] <= 131072
    unknown = cap.WindowCap(131072)
    try: unknown.rewrite(packet(version))
    except ValueError: pass
    else: raise AssertionError("unknown ACK accepted")
    bounded = cap.WindowCap(131072, max_flows=2)
    for port in (50000, 50001): bounded.rewrite(packet(version, flags=2, scale=4, port=port))
    try: bounded.rewrite(packet(version, flags=2, scale=4, port=50002))
    except ValueError: pass
    else: raise AssertionError("flow table exceeded")
    assert len(bounded.flows) == 2 and bounded.counts["flow_overflow"] == 1

    def rejected(rewriter, frame, counter):
        try:
            rewriter.rewrite(frame)
        except ValueError:
            assert rewriter.counts[counter] == 1
        else:
            raise AssertionError(f"invalid frame accepted: {counter}")

    rejected(cap.WindowCap(131072), packet(version, reverse=True, flags=0x12, scale=4), "unknown_flow")
    incomplete = cap.WindowCap(131072)
    incomplete.rewrite(packet(version, flags=2, scale=4))
    rejected(incomplete, packet(version), "unknown_flow")
    changed = cap.WindowCap(131072)
    syn = packet(version, flags=2, scale=4)
    assert changed.rewrite(syn) == syn and changed.rewrite(syn) == syn
    rejected(changed, packet(version, flags=2, scale=5), "changed_syn_offer")
    for options in ([3, 2, 4, 1], [3, 5, 4, 1], [1, 1, 1, 3], [3, 3, 4, 1, 3, 3, 4, 1], [3, 0, 0, 0]):
        malformed = packet(version, flags=2, raw_options=options)
        assert checksum(tcp_bytes(malformed)[1]) == 0
        rejected(cap.WindowCap(131072), malformed, "malformed_options")
    rejected(cap.WindowCap(131072), syn[:-1], "malformed_tcp")
    fragmented = bytearray(packet(version, flags=2))
    if version == 4:
        fragmented[20:22] = (0x2000).to_bytes(2, "big")
    else:
        fragmented[20] = 44
        fragmented[54:54] = bytes([6, 0, 0, 1, 0, 0, 0, 1])
        fragmented[18:20] = (len(fragmented) - 54).to_bytes(2, "big")
    rejected(cap.WindowCap(131072), bytes(fragmented), "malformed_tcp")
    disabled = cap.WindowCap(0)
    assert disabled.rewrite(packet(version)) == packet(version) and not disabled.flows

fixed_bound = cap.WindowCap(131072)
assert fixed_bound.max_flows == 128
for port in range(50000, 50128):
    fixed_bound.rewrite(packet(4, flags=2, scale=4, port=port))
rejected(fixed_bound, packet(4, flags=2, scale=4, port=50128), "flow_overflow")
assert len(fixed_bound.flows) == 128
print("IPv4/IPv6 window cap full-checksum, negotiated-scale, zero-window and bounds tests passed")
