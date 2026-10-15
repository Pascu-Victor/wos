#!/usr/bin/env python3
"""Strict saved-data decoder for the ordinary writable TMPFS fixture.

This decoder never qualifies VM identity, kernel identity or Host cleanup. Those
are separate runner obligations. Failure-prefix diagnostics earn no PASS credit.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import struct

MAGIC, GUARD = 0x35395752544D5046, 0x7C5E2198D3A06BF4
MASK = (1 << 64) - 1
PAGE, EOF_BYTES, LENGTH = 4096, 4169, 8192
WORDS, HEAD, CAPACITY, EW, CONTROL_BYTES = 512, 64, 44, 10, 32768
PARENT, PARENT_RO, CHILD, CHILD_RO = 0x4000000000, 0x4100000000, 0x5000000000, 0x5100000000
NO_CACHE, REGULAR, FIXED_SHARED = 0o100000000, 0o100000, 17
OPS = "GRANT SEED PREFILL MAP STORE READ_MAP PWRITE PROTECT COPY_TO COPY_FROM DENY SYNC REUSE FORK READY WAIT WAKE EXEC IMAGE WAITPID UNMAP GROW_BUSY TAIL GROW TAIL_READ CLEANUP CREATE REAP FLAGS".split()
OP = {name: index for index, name in enumerate(OPS, 1)}
ANY = object()


def need(condition, reason):
    if not condition:
        raise ValueError(reason)


def signed(n):
    return n if n < 1 << 63 else n - (1 << 64)


def marker(nonce, tag):
    return (nonce ^ ((tag * 0xD6E8FEB86659FD93) & MASK)) | 1


def checksum(data):
    value = 14695981039346656037
    for byte in data:
        value = ((value ^ byte) * 1099511628211) & MASK
    return value


def expected_bytes(nonce, tag):
    data = bytearray(LENGTH)
    struct.pack_into("<QQ", data, 0, marker(nonce, tag), marker(nonce, tag) ^ MASK)
    for index in range(73):
        data[PAGE + index] = ((nonce >> ((index % 8) * 8)) ^ (index * 37 + 19)) & 255
    return bytes(data)


def row(data, role, complete=True):
    need(len(data) == WORDS * 8, f"role{role}: exact bytes")
    h = struct.unpack(f"<{WORDS}Q", data)
    nonce, deadline, count = h[3], h[4], h[11]
    need(h[:3] == (MAGIC, 1, role), f"role{role}: magic/version/role")
    need(nonce & (1 << 63) and nonce & 1 and h[-1] == nonce, f"role{role}: nonce/commit")
    need(0 < h[7] <= h[8] and h[7] < deadline and deadline - h[7] <= 60_000_000,
         f"role{role}: absolute start/deadline")
    if complete:
        need(h[8] < deadline and h[9:11] == (0, 0), f"role{role}: deadline/failure")
    need(h[25:29] == (100, 100, 100, 100), f"role{role}: actual credentials")
    need(h[32:38] == (LENGTH, CAPACITY, WORDS * 8, 4, 1, 2), f"role{role}: shape/target sync ABI")
    need(not h[29] and not any(h[38:62]), f"role{role}: reserved header")
    need(h[62] == GUARD ^ nonce ^ role and h[63] == h[62] ^ MASK, f"role{role}: header guards")
    need(0 <= count <= CAPACITY and not any(h[HEAD + count * EW:-1]), f"role{role}: exact count/unused tail")
    events, previous = [], h[7]
    for index in range(count):
        event = h[HEAD + index * EW:HEAD + (index + 1) * EW]
        need(event[0] in OP.values() and event[1] == index + 1, f"role{role}: opcode/sequence")
        need(previous <= event[2] <= h[8] and event[9] == GUARD ^ nonce ^ role ^ event[1], f"role{role}: time/guard")
        if complete:
            need(event[2] < deadline, f"role{role}: event deadline")
        previous = event[2]
        events.append(event)
    return h, events


class Events:
    def __init__(self, decoded):
        self.h, self.rows = decoded
        self.index = 0

    def take(self, name, a=0, b=0, result=0, x=0, y=0, z=0):
        need(self.index < len(self.rows), f"role{self.h[2]}: missing {name}")
        event = self.rows[self.index]
        self.index += 1
        expected = (OP[name], a, b, result, x, y, z)
        actual = (event[0], *event[3:9])
        for column, (got, wanted) in enumerate(zip(actual, expected)):
            need(wanted is ANY or got == wanted & MASK,
                 f"role{self.h[2]} event{self.index} {name} field{column}: {got} != {wanted}")
        return event

    def end(self):
        need(self.index == len(self.rows), f"role{self.h[2]}: unexpected extra observations")


def grant(events, fd, identity, access, size, flags=None):
    e = events.take("GRANT", fd, ANY, 0, identity[0], identity[1], size)
    need(e[4] & 3 == access, "actual granted access")
    if flags is not None:
        need(e[4] == flags, "same retained File access flags")
    if size == EOF_BYTES:
        need(not e[4] & NO_CACHE, "ordinary data File must not gain O_NO_CACHE")
    return e[4]


def mapped(events, fd, prot, address):
    return events.take("MAP", fd, prot, address, LENGTH, FIXED_SHARED)


def observed(events, address, tag, name="READ_MAP"):
    value = marker(events.h[3], tag)
    return events.take(name, address, tag, 0, value, value ^ MASK, value)


def prefilled(events, tag):
    expected = checksum(expected_bytes(events.h[3], tag)[:EOF_BYTES])
    return events.take("PREFILL", 11, tag, EOF_BYTES, expected, expected, 0)


def control_record(data, nonce, deadline, seq, state, actor, value):
    need(len(data) == 512, "control record exact bytes")
    r = struct.unpack("<64Q", data)
    need(r[:8] == (MAGIC, 1, nonce, deadline, seq, state, actor, value), "control phase/identity/value")
    guard = GUARD ^ nonce ^ seq ^ state
    need(not any(r[8:61]) and r[61:] == (guard, guard ^ MASK, nonce), "control reserved/guards/commit")


def validate(rows, control):
    need(set(rows) == set(range(5)), "all five actual roles required")
    p = rows[1][0]
    nonce, deadline = p[3:5]
    backing, control_id = p[12:14], p[22:24]
    need(backing[1] != 0 and control_id[1] != 0 and backing != control_id, "distinct actual data/control File identities")
    need(p[14] == EOF_BYTES and p[17:20] == (PARENT, 0, PARENT_RO) and p[20] == 2 and p[21] > 2,
         "controller fixed EOF/independent geometry/real child")
    need(p[15] & 3 == 2 and p[16] & 3 == 0 and not (p[15] | p[16]) & NO_CACHE, "ordinary writable/readonly descriptions")
    need(p[30] & 0o170000 == REGULAR and p[31] & 0o170000 == REGULAR, "actual regular data/control types")
    expected_pids = {1: (1, 0), 2: (2, 1), 3: (2, 1), 4: (3, 2)}
    for role, (h, _) in rows.items():
        need(h[3:5] == (nonce, deadline), "common nonce/absolute deadline")
        if role:
            need(h[5:7] == expected_pids[role] and h[21] == p[21], "visible PID/PPID and physical CREATE identity")
            need(h[22:25] == (*control_id, CONTROL_BYTES) and h[31] == p[31], "same retained control description")
        if role in (2, 4):
            need(h[12:15] == p[12:15] and h[15] & 3 == 2 and h[16] & 3 == 0 and
                 not (h[15] | h[16]) & NO_CACHE and h[30] == p[30], "same object/ordinary independent descriptions")
    need(rows[2][0][17:21] == (PARENT, CHILD, CHILD_RO, 3), "independent child roots/VA/normal descendant")
    need(rows[4][0][12:17] == rows[2][0][12:17], "descendant retains exact child descriptions")
    for role, indices in {
        0: [*range(12, 20), *range(21, 25), 30, 31],
        1: [18], 2: [], 3: [*range(12, 21), 30], 4: [17, 18, 19, 20]
    }.items():
        need(all(rows[role][0][index] == 0 for index in indices), "role-specific unused header")
    h = rows[0][0]
    need(h[5] > 2 and h[20] == p[21] and h[7] <= p[7] and h[8] >= p[8], "Host encloses owned controller")
    need(rows[3][0][7] >= rows[2][0][8], "new image starts after old image report")
    need(rows[4][0][7] >= rows[2][0][7] and rows[4][0][8] <= rows[2][0][8], "descendant is enclosed by old child image")

    r = Events(rows[1])
    grant(r, 9, control_id, 2, CONTROL_BYTES)
    seed = expected_bytes(nonce, 1)
    r.take("SEED", EOF_BYTES, 0, EOF_BYTES, checksum(seed[:EOF_BYTES]), checksum(seed[PAGE:EOF_BYTES]), 0)
    grant(r, 10, backing, 2, EOF_BYTES, p[15])
    grant(r, 11, backing, 0, EOF_BYTES, p[16])
    prefilled(r, 1)
    mapped(r, 10, 3, PARENT)
    r.take("FLAGS", 10, 11, 0, p[15], p[16])
    r.take("REUSE", 10, 9, 10, *control_id, 0)
    fork = r.take("FORK", 0, 0, 2)
    store2 = observed(r, PARENT, 2, "STORE")
    read2 = prefilled(r, 2)
    m3 = marker(nonce, 3)
    write3 = r.take("PWRITE", 12, 0, 16, m3, m3 ^ MASK, m3)
    observed(r, PARENT, 3)
    prefilled(r, 3)
    observed(r, PARENT, 4)
    read4 = prefilled(r, 4)
    mapped(r, 11, 1, PARENT_RO)
    observed(r, PARENT_RO, 4)
    r.take("PROTECT", PARENT, 1, 0, LENGTH)
    m4 = marker(nonce, 4)
    r.take("COPY_TO", PARENT, 16, -14, m4, m4 ^ MASK, m4)
    r.take("PROTECT", PARENT, 0, 0, LENGTH)
    guard = GUARD ^ nonce
    r.take("COPY_FROM", PARENT, 16, -14, guard, guard ^ MASK, 0)
    r.take("PROTECT", PARENT, 3, 0, LENGTH)
    store5 = observed(r, PARENT, 5, "STORE")
    prefilled(r, 5)
    for op in range(1, 5):
        r.take("DENY", op, 0, -13)
    for flags in (4, 1, 6, 8, 5):
        r.take("SYNC", PARENT, flags, -22 if flags in (8, 5) else 0, LENGTH)
    r.take("GROW_BUSY", LENGTH, EOF_BYTES, -16, EOF_BYTES, 0)
    r.take("TAIL", PARENT + EOF_BYTES, LENGTH - EOF_BYTES, 0, 0xA5, checksum(bytes([0xA5]) * (LENGTH - EOF_BYTES)))
    store6 = observed(r, PARENT, 6, "STORE")
    wake = r.take("WAKE", PARENT + 128, ANY, 1, CHILD_RO + 128)
    need(1 <= wake[4] <= 128, "actual bounded terminal wake1")
    reaped = r.take("WAITPID", 2, 0, 2)
    r.take("UNMAP", PARENT, LENGTH)
    r.take("UNMAP", PARENT_RO, LENGTH)
    grow = r.take("GROW", LENGTH, ANY, 0, LENGTH, 0)
    need(1 <= grow[4] <= 64, "bounded actual EOF release retry")
    final = expected_bytes(nonce, 6)
    r.take("TAIL_READ", EOF_BYTES, LENGTH - EOF_BYTES, 0, checksum(final), checksum(bytes(LENGTH - EOF_BYTES)), checksum(final[PAGE:EOF_BYTES]))
    r.take("CLEANUP", 10, 12)
    r.end()
    need(p[11] == 44, "complete compact controller census")

    c = Events(rows[2])
    grant(c, 9, control_id, 2, CONTROL_BYTES)
    c.take("UNMAP", PARENT, LENGTH)
    grant(c, 13, backing, 2, EOF_BYTES, c.h[15])
    mapped(c, 13, 3, CHILD)
    observed(c, CHILD, 1)
    ready = c.take("READY", CHILD)
    seen2 = observed(c, CHILD, 2)
    seen3 = observed(c, CHILD, 3)
    child_store = observed(c, CHILD, 4, "STORE")
    grant(c, 14, backing, 0, EOF_BYTES, c.h[16])
    mapped(c, 14, 1, CHILD_RO)
    ro_seen = observed(c, CHILD_RO, 5)
    waited = c.take("WAIT", CHILD_RO + 128, ANY, 0)
    need(0 < waited[4] <= 5_000_000, "bounded actual WAIT timeout")
    wake_seen = observed(c, CHILD_RO, 6)
    descendant = c.take("FORK", 1, 0, 3)
    c.take("WAITPID", 3, 0, 3)
    exec_intent = c.take("EXEC", 2, CHILD, 0, CHILD_RO)
    c.end()
    need(c.h[11] == 17, "complete child census")
    # Data checks follow actual nonoverlapping control handoffs; no merely
    # concurrent read is used as the sequential cache-authority proof.
    need(fork[2] <= ready[2] <= store2[2] <= seen2[2] <= read2[2] <= write3[2] <= seen3[2] <= child_store[2] <= read4[2],
         "synchronized initial/write/read authority ordering")
    need(store5[2] <= ro_seen[2] <= store6[2] <= wake[2] and store6[2] <= waited[2] <= wake_seen[2] <= descendant[2],
         "independent readonly alias and actual wake/ACK ordering")
    need(exec_intent[2] <= rows[2][0][8] <= rows[3][0][7] <= rows[3][0][8] <= reaped[2] <= grow[2],
         "real exec/reap precedes final EOF release")

    e = Events(rows[3])
    grant(e, 9, control_id, 2, CONTROL_BYTES)
    e.take("IMAGE", 2, CHILD, -14, guard, guard ^ MASK, 0)
    e.end()
    g = Events(rows[4])
    grant(g, 9, control_id, 2, CONTROL_BYTES)
    grant(g, 13, backing, 2, EOF_BYTES, c.h[15])
    grant(g, 14, backing, 0, EOF_BYTES, c.h[16])
    observed(g, CHILD, 6)
    observed(g, CHILD_RO, 6)
    g.end()
    host = Events(rows[0])
    host.take("CREATE", 1, 0, p[21], 1)
    host.take("REAP", p[21], 0, p[21])
    host.end()
    need(rows[3][0][11] == 2 and rows[4][0][11] == 5 and rows[0][0][11] == 2, "remaining role census")

    need(len(control) == CONTROL_BYTES, "control exact bytes")
    used = bytearray(CONTROL_BYTES)
    for offset, seq, state, actor, value in (
        (0, 0, 1, rows[0][0][5], p[21]),
        (512, 0, 1, 2, CHILD),
        (1024, 3, 3, 1, marker(nonce, 5)),
        (1536, 3, 3, 2, marker(nonce, 6)),
        (2048, 3, 3, 2, CHILD_RO),
    ):
        control_record(control[offset:offset + 512], nonce, deadline, seq, state, actor, value)
        used[offset:offset + 512] = b"\1" * 512
    need(control[4096:4128] == struct.pack("<4Q", guard, 0, 0, guard ^ MASK), "guarded failed-copy scratch unchanged")
    used[4096:4128] = b"\1" * 32
    for role in range(1, 5):
        offset = 8192 + role * WORDS * 8
        need(control[offset:offset + WORDS * 8] == struct.pack(f"<{WORDS}Q", *rows[role][0]), "raw retained report exact copy")
        used[offset:offset + WORDS * 8] = b"\1" * (WORDS * 8)
    need(not any(byte for index, byte in enumerate(control) if not used[index]), "zero unused control/padding")
    return {
        "qualification": "COMPLETE_OFFLINE_WRITESHARED_PROTOCOL_ONLY",
        "protocol_complete": True, "vm_identity_qualified": False, "public_runtime_qualified": False,
        "nonce": nonce, "deadline_us": deadline, "roles": 5, "events": 70,
        "event_counts": [rows[role][0][11] for role in range(5)],
        "writable_independent_roots": True, "ordinary_prefilled_sequential_reader": True,
        "shared_wait_result": 0, "terminal_wake_count": 1,
        "partial_eof_tail_zero_after_scoped_release": True,
        "global_reference_drain_proved": False, "full_goal_criteria_qualified": False,
    }



def init_snapshot(data, status):
    need(len(data) == 4160, "exact init ABI snapshot")
    size, version, state, flags, sequence, published, count, reserved, *rest = struct.unpack_from("<IIIIQQIIQQQ", data)
    need((size, version) == (4160, 1) and 0 <= state <= 5 and flags & ~1 == 0 and sequence > 0 and published > 0,
         "init snapshot header")
    need(1 <= count <= 16 and reserved == 0 and not any(rest), "init count/reserved")
    targets = []
    for index in range(count):
        offset = 64 + index * 256
        s = data[offset:offset + 256]
        name, separator, tail = s[:64].partition(b"\0")
        need(separator and not any(tail), "init bounded zero-padded name")
        st, rd, sf, rz, pid, pgid, gen, restart, wait, error, since, due, hc, rz2, rz3, rz4 = struct.unpack_from("<IIIIqqQQiiQQIIQQ", s, 64)
        need(st <= 8 and rd <= 3 and sf & ~7 == 0 and not any((rz, rz2, rz3, rz4)) and hc <= 4, "service fields/reserved")
        need(not any(s[160 + hc * 24:]), "service unused history")
        history = [struct.unpack_from("<QIIII", s, 160 + i * 24) for i in range(hc)]
        need(all(0 < h[0] <= published and h[1] <= 8 and h[2] <= 8 and h[3] <= 9 and h[4] == 0 for h in history), "history bounds")
        if name == b"tmpfs-writeshared-probe":
            need((st, rd, pid, pgid, gen, restart, wait, error) == (6, 2, -1, -1, 1, 0, 0, 0), "exact probe exited successfully")
            need(any(h[2:4] == (6, 5) for h in history), "actual init EXIT transition")
            targets.append(name)
    need(len(targets) == 1 and not any(data[64 + count * 256:]), "one target and zero unused services")
    expected = (f"PASS tmpfs-writeshared-init-exit service=tmpfs-writeshared-probe generation=1 restarts=0 sequence={sequence} "
                f"published-ns={published} state=6 readiness=2 wait=0 error=0 history-exit=1 snapshot-bytes=4160 abi=1 read-error=0 failures=0\n")
    padded_line(status, expected)
    return {"sequence": sequence, "published_ns": published, "probe_reaped_by_init": True}


def padded_line(data, expected):
    need(len(data) == 512 and data == expected.encode() + bytes(512 - len(expected)), "exact padded status line")


def decode(directory):
    directory = Path(directory)
    names = ["host.bin", *(f"role-{role}.bin" for role in range(1, 5)), "control.bin", "status"]
    data = {}
    for name in names:
        p = directory / name
        expected = CONTROL_BYTES if name == "control.bin" else 512 if name == "status" else WORDS * 8
        need(p.is_file() and not p.is_symlink() and p.stat().st_size == expected, f"exact regular input {name}")
        raw = p.read_bytes()
        need(len(raw) == expected, f"stable exact input {name}")
        data[name] = raw
    rows = {role: row(data["host.bin" if role == 0 else f"role-{role}.bin"], role) for role in range(5)}
    result = validate(rows, data["control.bin"])
    status = f"PUBLIC_COMPLETE tmpfs-writeshared nonce={result['nonce']} failures=0\n".encode()
    need(data["status"] == status + bytes(512 - len(status)), "exact fixed status/zero padding")
    result["artifacts"] = {name: {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()} for name, raw in data.items()}
    return result


def diagnostic(directory):
    output = {"protocol_complete": False, "vm_identity_qualified": False, "public_runtime_qualified": False, "roles": {}}
    for role in range(5):
        path = Path(directory) / ("host.bin" if role == 0 else f"role-{role}.bin")
        if not path.is_file() or path.is_symlink() or path.stat().st_size != WORDS * 8:
            continue
        try:
            h, events = row(path.read_bytes(), role, complete=False)
            output["roles"][str(role)] = {"pid": h[5], "failures": h[9], "first_failure_stage": h[10],
                                         "events": [[OPS[e[0] - 1], *e[1:9]] for e in events]}
        except (ValueError, OSError, struct.error) as error:
            output["roles"][str(role)] = {"error": str(error)}
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--diagnostic", action="store_true", help="decode available prefixes; always exits nonzero")
    args = parser.parse_args()
    try:
        if args.diagnostic:
            print(json.dumps(diagnostic(args.directory), sort_keys=True, indent=2))
            return 1
        print(json.dumps(decode(args.directory), sort_keys=True, indent=2))
        return 0
    except (ValueError, OSError, struct.error) as error:
        print(json.dumps({"protocol_complete": False, "vm_identity_qualified": False,
                          "public_runtime_qualified": False, "error": str(error)}, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
