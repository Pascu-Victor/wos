#!/usr/bin/env python3
"""Bounded Ethernet impairment bridge between two private QEMU socket NICs.

Both endpoints use node_setup's socket-mcast backend with queues=1,
vhost=false, and localaddr=127.0.0.1. Give the endpoints distinct multicast
groups/ports and pass their exact MAC addresses. Only frames originating at
the corresponding VM are forwarded, excluding the bridge's own loopbacks.
Loss/duplication/reordering target TCP payload packets; ARP/NDP, handshake,
and ACK packets retain the configured delay and bandwidth limit.
"""

import argparse
from collections import Counter
import heapq
import ipaddress
import json
import math
from pathlib import Path
import selectors
import signal
import socket
import time

FINAL_SPIN_SECONDS = 0.0001


def service_plan(remaining):
    """Avoid epoll's positive sub-ms timeout rounding at a packet deadline."""
    if remaining is None or remaining > 0.001 + FINAL_SPIN_SECONDS:
        return "select", 0.001
    if remaining > FINAL_SPIN_SECONDS:
        return "sleep", remaining - FINAL_SPIN_SECONDS
    return "spin", min(FINAL_SPIN_SECONDS, max(remaining, 0))


def wait_events(selector, directions, precise):
    if not precise:
        return selector.select(0.001)
    deadlines = [direction.queue[0][0] for direction in directions if direction.queue]
    deadlines += [direction.held[0] for direction in directions if direction.held]
    due = min(deadlines) if deadlines else None
    operation, duration = service_plan(None if due is None else due - time.monotonic())
    if operation == "select":
        return selector.select(duration)
    ready = selector.select(0)
    if ready:
        return ready
    if operation == "sleep":
        time.sleep(duration)
        return selector.select(0)
    # This final interval is capped at 100us; no unbounded busy wait. Incoming
    # datagrams interrupt it so receive admission remains responsive.
    end = time.monotonic() + duration
    while time.monotonic() < end:
        ready = selector.select(0)
        if ready:
            return ready
    return []


def endpoint(value):
    address, port = value.rsplit(":", 1)
    parsed = ipaddress.ip_address(address)
    port = int(port)
    if parsed.version != 4 or not parsed.is_multicast or not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("expected multicast IPv4:port")
    return address, port


def mac(value):
    try:
        result = bytes.fromhex(value.replace(":", ""))
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected six-byte MAC address") from error
    if len(result) != 6 or result[0] & 1:
        raise argparse.ArgumentTypeError("expected six-byte unicast MAC address")
    return result


def tcp_payload(frame):
    """Return (IP version, payload length); ignore malformed/fragments/options.

    IPv6 extensions are deliberately outside this focused fixture. IPv4 IP
    options are supported. Lengths are bounded by the encoded IP datagram,
    rather than Ethernet padding.
    """
    if len(frame) < 14:
        return None
    kind = int.from_bytes(frame[12:14], "big")
    if kind == 0x0800:
        if len(frame) < 34 or frame[14] >> 4 != 4 or frame[23] != 6:
            return None
        header = (frame[14] & 15) * 4
        total = int.from_bytes(frame[16:18], "big")
        fragments = int.from_bytes(frame[20:22], "big")
        if header < 20 or fragments & 0x3FFF or total < header + 20 or len(frame) < 14 + total:
            return None
        offset, end, version = 14 + header, 14 + total, 4
    elif kind == 0x86DD:
        if len(frame) < 74 or frame[14] >> 4 != 6 or frame[20] != 6:
            return None
        total = int.from_bytes(frame[18:20], "big")
        if total < 20 or len(frame) < 54 + total:
            return None
        offset, end, version = 54, 54 + total, 6
    else:
        return None
    header = (frame[offset + 12] >> 4) * 4
    if header < 20 or offset + header > end:
        return None
    return version, end - offset - header


def checksum_replace16(checksum, old, new):
    value = (~checksum & 0xffff) + (~old & 0xffff) + new
    value = (value & 0xffff) + (value >> 16)
    value = (value & 0xffff) + (value >> 16)
    return ~value & 0xffff


class WindowCap:
    """Bound advertised credit using the window scale negotiated by both SYNs."""

    def __init__(self, limit, max_flows=128):
        self.limit = limit
        self.max_flows = max_flows
        self.flows = {}
        self.counts = Counter()

    def fail(self, counter, message):
        self.counts[counter] += 1
        raise ValueError(message)

    def rewrite(self, frame):
        if not self.limit:
            return frame
        parsed = tcp_payload(frame)
        if parsed is None:
            kind = int.from_bytes(frame[12:14], "big") if len(frame) >= 14 else 0
            if ((kind == 0x0800 and len(frame) >= 24 and frame[23] == 6) or
                    (kind == 0x86dd and len(frame) >= 21 and frame[20] == 6) or
                    (kind == 0x86dd and len(frame) >= 55 and frame[20] == 44 and frame[54] == 6)):
                self.fail("malformed_tcp", "window cap cannot classify TCP frame")
            return frame
        version, _ = parsed
        offset = 14 + (frame[14] & 15) * 4 if version == 4 else 54
        source_address = frame[26:30] if version == 4 else frame[22:38]
        target_address = frame[30:34] if version == 4 else frame[38:54]
        source = (source_address, frame[offset:offset + 2])
        target = (target_address, frame[offset + 2:offset + 4])
        key = (version, min(source, target), max(source, target))
        side = 0 if source == key[1] else 1
        flags = frame[offset + 13]
        window = int.from_bytes(frame[offset + 14:offset + 16], "big")
        if flags & 0x02:
            if flags & 0x10 and (key not in self.flows or self.flows[key][1 - side] is None):
                self.fail("unknown_flow", "window cap saw SYNACK before peer SYN offer")
            if key not in self.flows:
                if len(self.flows) >= self.max_flows:
                    self.fail("flow_overflow", "window cap flow table exhausted")
                self.flows[key] = [None, None]
                self.counts["tracked_flows"] = len(self.flows)
            offer = (False, 0)
            cursor, end = offset + 20, offset + (frame[offset + 12] >> 4) * 4
            while cursor < end:
                kind = frame[cursor]
                if kind == 0:
                    break
                if kind == 1:
                    cursor += 1
                    continue
                if cursor + 2 > end or frame[cursor + 1] < 2 or cursor + frame[cursor + 1] > end:
                    self.fail("malformed_options", "window cap saw malformed SYN options")
                length = frame[cursor + 1]
                if kind == 3:
                    if length != 3 or offer[0]:
                        self.fail("malformed_options", "window cap saw malformed window-scale option")
                    offer = (True, min(frame[cursor + 2], 14))
                cursor += length
            previous = self.flows[key][side]
            if previous is not None and previous != offer:
                self.fail("changed_syn_offer", "window scale changed on a repeated SYN")
            self.flows[key][side] = offer
            self.counts[f"ipv{version}_syn_packets"] += 1
            return frame  # SYN/SYNACK windows always use the unscaled word.
        if not flags & 0x10:
            return frame
        offers = self.flows.get(key)
        if offers is None or any(offer is None for offer in offers):
            self.fail("unknown_flow", "window cap saw ACK before both SYN offers")
        scale = offers[side][1] if all(offer[0] for offer in offers) else 0
        replacement = min(window, min(0xffff, self.limit >> scale))
        self.counts[f"ipv{version}_ack_packets"] += 1
        maximum = f"ipv{version}_maximum_effective_window"
        self.counts[maximum] = max(self.counts[maximum], replacement << scale)
        if replacement == window:
            return frame
        changed = bytearray(frame)
        checksum = int.from_bytes(frame[offset + 16:offset + 18], "big")
        changed[offset + 14:offset + 16] = replacement.to_bytes(2, "big")
        changed[offset + 16:offset + 18] = checksum_replace16(checksum, window, replacement).to_bytes(2, "big")
        self.counts[f"ipv{version}_rewritten_windows"] += 1
        return bytes(changed)

class Direction:
    """FIFO bandwidth service plus finite deterministic impairment holding."""

    def __init__(self, args):
        self.args = args
        self.queue = []
        self.queued_bytes = 0
        self.serial = 0
        self.last_finish = 0.0
        self.held = None
        self.counts = Counter()
        self.windows = {}

    def enqueue(self, frame, now):
        if (len(self.queue) + bool(self.held) >= self.args.max_packets or
                self.queued_bytes + len(frame) > self.args.max_bytes):
            self.counts["queue_overflow"] += 1
            return
        service = len(frame) * 8 / (self.args.rate_mbps * 1_000_000) if self.args.rate_mbps else 0
        self.last_finish = max(now, self.last_finish) + service
        due = self.last_finish + self.args.delay_ms / 1000
        self.serial += 1
        heapq.heappush(self.queue, (due, self.serial, frame))
        self.queued_bytes += len(frame)
        self.counts["queue_peak_packets"] = max(self.counts["queue_peak_packets"], len(self.queue) + bool(self.held))
        self.counts["queue_peak_bytes"] = max(self.counts["queue_peak_bytes"], self.queued_bytes)

    def admit(self, frame, now):
        self.counts["received"] += 1
        parsed = tcp_payload(frame)
        ordinal = 0
        if parsed:
            version, _ = parsed
            offset = 14 + (frame[14] & 15) * 4 if version == 4 else 54
            key = (version, frame[offset:offset + 4])
            window = int.from_bytes(frame[offset + 14:offset + 16], "big")
            if frame[offset + 13] & 0x10:
                previous = self.windows.get(key)
                if window == 0:
                    self.counts[f"ipv{version}_zero_window_announcements"] += 1
                    if previous != 0:
                        self.counts[f"ipv{version}_zero_window_episodes"] += 1
                elif previous == 0:
                    self.counts[f"ipv{version}_window_reopened"] += 1
                if key not in self.windows and len(self.windows) >= 256:
                    self.windows.pop(next(iter(self.windows)))
                    self.counts["window_tracking_evictions"] += 1
                self.windows[key] = window
            if frame[offset + 13] & 0x02:
                cursor, end = offset + 20, offset + (frame[offset + 12] >> 4) * 4
                while cursor < end:
                    kind = frame[cursor]
                    if kind == 0:
                        break
                    if kind == 1:
                        cursor += 1
                        continue
                    if cursor + 2 > end or frame[cursor + 1] < 2 or cursor + frame[cursor + 1] > end:
                        break
                    length = frame[cursor + 1]
                    if kind == 2 and length == 4:
                        mss = int.from_bytes(frame[cursor + 2:cursor + 4], "big")
                        minimum = f"ipv{version}_syn_mss_min"
                        maximum = f"ipv{version}_syn_mss_max"
                        self.counts[minimum] = min(self.counts[minimum] or mss, mss)
                        self.counts[maximum] = max(self.counts[maximum], mss)
                    cursor += length
        if parsed and parsed[1] > 0:
            version, payload = parsed
            self.counts[f"ipv{version}_data_packets"] += 1
            self.counts[f"ipv{version}_data_bytes"] += payload
            if payload == 1:
                self.counts[f"ipv{version}_one_byte_data"] += 1
            self.counts["data_packets"] += 1
            # Use the same ordinal trace independently for IPv4 and IPv6.
            ordinal = self.counts[f"ipv{version}_data_packets"]
        if ordinal and self.args.drop_every and ordinal % self.args.drop_every == 0:
            self.counts["loss_dropped"] += 1
            return
        if ordinal and self.args.reorder_every and ordinal % self.args.reorder_every == 0 and not self.held:
            if len(self.queue) >= self.args.max_packets or self.queued_bytes + len(frame) > self.args.max_bytes:
                self.counts["queue_overflow"] += 1
                return
            self.held = (now + self.args.reorder_hold_ms / 1000, frame)
            self.queued_bytes += len(frame)
            self.counts["reorder_held"] += 1
            return
        self.enqueue(frame, now)
        if ordinal and self.args.duplicate_every and ordinal % self.args.duplicate_every == 0:
            self.enqueue(frame, now)
            self.counts["duplicate_attempts"] += 1
        if self.held:
            _, held = self.held
            self.held = None
            self.queued_bytes -= len(held)
            self.enqueue(held, now)
            self.counts["reorder_released"] += 1

    def release(self, now):
        if self.held and self.held[0] <= now:
            _, frame = self.held
            self.held = None
            self.queued_bytes -= len(frame)
            self.enqueue(frame, now)
            self.counts["reorder_timeout_released"] += 1
        ready = []
        while self.queue and self.queue[0][0] <= now:
            due, _, frame = heapq.heappop(self.queue)
            lateness = max(0, round((now - due) * 1_000_000_000))
            parsed = tcp_payload(frame)
            if parsed:
                version, _ = parsed
                offset = 14 + (frame[14] & 15) * 4 if version == 4 else 54
                ports = (int.from_bytes(frame[offset:offset + 2], "big"), int.from_bytes(frame[offset + 2:offset + 4], "big"))
                scope = f"ipv{version}_{'tiny' if 49100 + version in ports else 'tcp'}"
                prefix = f"lateness_{scope}"
                self.counts[prefix + "_packets"] += 1
                self.counts[prefix + "_sum_ns"] += lateness
                self.counts[prefix + "_max_ns"] = max(self.counts[prefix + "_max_ns"], lateness)
                for limit in (50000, 100000, 200000, 500000, 1000000):
                    if lateness <= limit:
                        self.counts[f"{prefix}_le_{limit}ns"] += 1
            self.queued_bytes -= len(frame)
            ready.append(frame)
        return ready


def open_backend(address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # One sender may admit a full 1MiB flight before the event loop runs.
        # Stay within the host's normal socket limit; no privileged sysctl.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 7 * 1024 * 1024)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 7 * 1024 * 1024)
        sock.bind(address)
        loopback = socket.inet_aton("127.0.0.1")
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton(address[0]) + loopback)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, loopback)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 0)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        sock.setblocking(False)
        return sock
    except BaseException:
        sock.close()
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--left", type=endpoint, required=True)
    parser.add_argument("--right", type=endpoint, required=True)
    parser.add_argument("--left-mac", type=mac, required=True)
    parser.add_argument("--right-mac", type=mac, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=float, default=120)
    parser.add_argument("--rate-mbps", type=float, default=0)
    parser.add_argument("--delay-ms", type=float, default=0)
    parser.add_argument("--drop-every", type=int, default=0)
    parser.add_argument("--duplicate-every", type=int, default=0)
    parser.add_argument("--reorder-every", type=int, default=0)
    parser.add_argument("--reorder-hold-ms", type=float, default=25)
    parser.add_argument("--max-packets", type=int, default=1024)
    parser.add_argument("--max-bytes", type=int, default=8 * 1024 * 1024)
    parser.add_argument("--precise-deadlines", action="store_true", help="Bound final deadline spin to 100us instead of rounded sub-ms epoll sleeps")
    parser.add_argument("--window-cap-bytes", type=int, choices=(0, 131072), default=0,
                        help="Bound negotiated advertised TCP credit (0 disables, 131072 is 128KiB)")
    args = parser.parse_args()
    if args.left == args.right or args.left_mac == args.right_mac:
        parser.error("endpoints and MAC addresses must be distinct")
    if not all(math.isfinite(value) for value in (args.seconds, args.rate_mbps, args.delay_ms, args.reorder_hold_ms)) or args.seconds <= 0 or min(args.rate_mbps, args.delay_ms, args.reorder_hold_ms) < 0:
        parser.error("duration must be positive; rate and delays must be nonnegative")
    if min(args.drop_every, args.duplicate_every, args.reorder_every) < 0 or min(args.max_packets, args.max_bytes) <= 0:
        parser.error("ordinals must be nonnegative and queue bounds positive")
    if args.output.exists():
        parser.error("output already exists; use a fresh receipt path")
    stop = False

    def interrupt(_signum, _frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, interrupt)
    signal.signal(signal.SIGINT, interrupt)
    directions = [Direction(args), Direction(args)]
    window_cap = WindowCap(args.window_cap_bytes)
    addresses, expected = [args.left, args.right], [args.left_mac, args.right_mac]
    sockets = []
    started = time.monotonic()
    errors = []
    try:
        with selectors.DefaultSelector() as selector:
            for index, address in enumerate(addresses):
                sock = open_backend(address)
                sockets.append(sock)
                selector.register(sock, selectors.EVENT_READ, index)
            print("tcp-impairment: READY", flush=True)
            while not stop and time.monotonic() - started < args.seconds:
                for key, _ in wait_events(selector, directions, args.precise_deadlines):
                    index = key.data
                    # Bound each service pass so one busy NIC cannot starve
                    # the reverse direction or delay pending egress packets.
                    for _ in range(64):
                        try:
                            frame, _ = key.fileobj.recvfrom(65536)
                        except BlockingIOError:
                            break
                        if len(frame) < 14 or frame[6:12] != expected[index]:
                            directions[index].counts["ignored_loopback_or_foreign"] += 1
                            continue
                        directions[index].admit(window_cap.rewrite(frame), time.monotonic())
                now = time.monotonic()
                for index, direction in enumerate(directions):
                    for frame in direction.release(now):
                        sent = sockets[1 - index].sendto(frame, addresses[1 - index])
                        if sent != len(frame):
                            raise RuntimeError("short Ethernet datagram send")
                        direction.counts["forwarded"] += 1
    except Exception as error:
        errors.append(f"{type(error).__name__}: {error}")
    finally:
        for sock in sockets:
            sock.close()
        receipt = {
            "format": "wos-tcp-impairment-v1", "duration_s": time.monotonic() - started,
            "policy": {key: value for key, value in vars(args).items() if key not in ("output", "left_mac", "right_mac")},
            "macs": [value.hex(":") for value in expected],
            "directions": [dict(direction.counts, pending_packets=len(direction.queue) + bool(direction.held),
                                pending_bytes=direction.queued_bytes) for direction in directions],
            "errors": errors,
            "window_cap": dict(window_cap.counts, max_flows=window_cap.max_flows),
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(receipt, indent=2) + "\n")
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
