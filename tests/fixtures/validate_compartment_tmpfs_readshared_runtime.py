#!/usr/bin/env python3
"""Strict offline decoder for the separate public readonly TMPFS fixture.

--selftest uses synthetic parser inputs only; it cannot qualify guest execution.
Actual directory validation checks every row/event/control and the init witness.
Source/artifact/VM identity and storage admission remain the external runner's job.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import struct

MAGIC = 0x3538524F544D5046
VERSION = 1
GUARD = 0x6D41A8C297E53B0F
MASK = (1 << 64) - 1
PAGE, LENGTH = 4096, 129 * 4096
WORDS, CAPACITY, HEAD, EW, CONTROL_BYTES = 4096, 256, 64, 12, 147456
NO_CACHE, REGULAR = 0o100000000, 0o100000
OPS = "GRANT TRUNCATE MAP READ PWRITE COPY PROTECT THREAD_CREATE THREAD_JOIN THREAD_READ THREAD_PROTECT CLOSE DUP FORK READY WAIT WAKE ACK EXEC_INTENT NEW_IMAGE EXIT_INTENT WAITPID UNMAP GROW_RELEASE CREATE REAP PREAD REFUSE COPY_NONE".split()
OP = {name: number for number, name in enumerate(OPS, 1)}
ANY = object()


def need(condition, reason):
    if not condition:
        raise ValueError(reason)


def signed(value):
    return value if value < 1 << 63 else value - (1 << 64)


def marker(nonce, tag):
    return (nonce ^ ((tag * 0xD6E8FEB86659FD93) & MASK)) | 1


def complement(value):
    return value ^ MASK if value else 0


def decoded_row(data, role):
    need(len(data) == WORDS * 8, f"role{role}: exact row bytes")
    h = struct.unpack(f"<{WORDS}Q", data)
    nonce, deadline, count = h[3], h[4], h[9]
    need(h[:3] == (MAGIC, VERSION, role), f"role{role}: magic/version/role")
    need(nonce & (1 << 63) and nonce & 1 and h[-1] == nonce, f"role{role}: nonce/commit")
    need(0 < h[7] <= h[8] < deadline and deadline - h[7] <= 60_000_000, f"role{role}: interval")
    need(h[10:12] == (0, 0) and h[29:31] == (WORDS * 8, CAPACITY), f"role{role}: failures/shape")
    need(h[20:27] == (100, 100, 100, 100, 1, 0, 256), f"role{role}: identities/authority")
    need(h[31] == 0 and not any(h[33:62]), f"role{role}: reserved header")
    need(h[62] == GUARD ^ nonce ^ role and h[63] == h[62] ^ MASK, f"role{role}: guards")
    need(0 < count <= CAPACITY and not any(h[HEAD + count * EW:-1]), f"role{role}: count/tail")
    events = []
    previous = h[7]
    for index in range(count):
        event = h[HEAD + index * EW:HEAD + (index + 1) * EW]
        need(event[0] in OP.values() and event[1] == index + 1, f"role{role}: event opcode/sequence")
        need(previous <= event[2] <= h[8] and event[11] == GUARD ^ nonce ^ role ^ event[1],
             f"role{role}: event time/guard")
        previous = event[2]
        events.append(event)
    return h, events


class Events:
    def __init__(self, row):
        self.h, self.rows = row
        self.index = 0

    def peek(self, name):
        return self.index < len(self.rows) and self.rows[self.index][0] == OP[name]

    def take(self, name, a=0, b=0, result=0, x=0, y=0, z=0, w=0, v=0):
        need(self.index < len(self.rows), f"role{self.h[2]}: missing {name}")
        event = self.rows[self.index]
        self.index += 1
        expected = (OP[name], a, b, result, x, y, z, w, v)
        actual = (event[0], *event[3:11])
        for column, (got, wanted) in enumerate(zip(actual, expected)):
            need(wanted is ANY or got == (wanted & MASK),
                 f"role{self.h[2]} event{self.index} {name} field{column}: {got} != {wanted}")
        return event

    def finished(self):
        need(self.index == len(self.rows), f"role{self.h[2]}: extra events")


def grant(reader, fd, identity, access, size):
    e = reader.take("GRANT", fd, ANY, 0, identity[0], identity[1], size, ANY, 0)
    need(e[4] & 3 == access and e[4] & NO_CACHE and e[9] & 0o170000 == REGULAR,
         "actual grant access/cache/type")
    return e


def observed(reader, address, offset, expected, stage, name="READ"):
    return reader.take(name, address, offset, 0, expected, complement(expected), expected, stage)


def message(data, nonce, deadline, sequence, state):
    need(len(data) == 512, "control record bytes")
    r = struct.unpack("<64Q", data)
    need(r[:6] == (MAGIC, VERSION, nonce, deadline, sequence, state), "control record identity/phase")
    need(not any(r[8:61]) and r[61] == GUARD ^ nonce ^ sequence and r[62] == r[61] ^ MASK and r[63] == nonce,
         "control reserved/guards/commit")
    return r


def validate_rows(rows, control):
    need(set(rows) == set(range(5)), "all five actual roles")
    h, events = rows[1]
    nonce, deadline = h[3:5]
    backing, control_id = h[12:14], h[27:29]
    need(backing != control_id and backing[1] != 0 and control_id[1] != 0, "distinct backing/control identity")
    need(h[14] == LENGTH and h[15] & 3 == 0 and h[15] & NO_CACHE, "readonly fixed-EOF source header")
    parent_a, parent_b = h[16:18]
    need(parent_a > 0 and parent_b > 0 and parent_a != parent_b and (parent_a | parent_b) % PAGE == 0,
         "parent exact distinct aligned mappings")
    need(parent_a + LENGTH <= 1 << 47 and parent_b + LENGTH <= 1 << 47 and
         (parent_a + LENGTH <= parent_b or parent_b + LENGTH <= parent_a), "parent disjoint userspace extents")
    expected_pids = {1: (1, 0), 2: (3, 1), 3: (3, 1), 4: (4, 1)}
    physical = h[32]
    need(physical > 2 and rows[0][0][5] > 2, "actual Host/CREATE positive identities")
    for role, (header, _) in rows.items():
        need(header[3:5] == (nonce, deadline), "common run nonce/deadline")
        if role:
            need(header[5:7] == expected_pids[role] and header[32] == physical, "actual local process identity")
            need(header[27:29] == control_id, "same retained control File")
        if role in (2, 4):
            need(header[12:16] == h[12:16] and header[16] == parent_a, "fork exact inherited backing/VA")
        unused = {0: [*range(12, 18), 19, 27, 28, 32], 1: [], 2: [18, 19],
                  3: list(range(12, 20)), 4: [17, 18, 19]}[role]
        need(all(header[i] == 0 for i in unused), "role-specific unused header fields")
    need(h[18:20] == (3, 4) and rows[0][0][18] == physical, "actual fork/CREATE ownership")
    need(rows[3][0][7] >= rows[2][0][8], "replacement starts after old-image row publication")
    need(rows[0][0][7] <= h[7] and rows[0][0][8] >= h[8], "Host encloses controller")
    initial, hot, woke = (marker(nonce, i) for i in (1, 2, 3))
    r = Events(rows[1])
    grant(r, 9, control_id, 2, CONTROL_BYTES)
    for fd, access in ((10, 2), (11, 0)):
        if fd == 11:
            # Initial sparse truncate/seed precede the independent readonly open.
            r.take("TRUNCATE", 10, LENGTH, 0, LENGTH)
            r.take("PWRITE", 10, 0, 16, initial, complement(initial), initial)
        if r.peek("DUP"):
            e = r.take("DUP", ANY, fd, fd)
            need(0 <= e[3] < 256 and e[3] not in (9, 10, 11), "owned temporary descriptor")
            r.take("CLOSE", e[3])
    grant(r, 10, backing, 2, LENGTH)
    grant(r, 11, backing, 0, LENGTH)
    r.take("MAP", LENGTH, 1, parent_a, 11, 0, 1, 1)
    r.take("THREAD_CREATE", 65536)
    r.take("MAP", LENGTH, 0, parent_b, 11, 0, 1, 2)
    r.take("PROTECT", parent_b, LENGTH, 0, 1)
    r.take("THREAD_JOIN", 1)
    r.take("THREAD_READ", parent_a, 1, 0, initial, complement(initial), initial)
    r.take("THREAD_PROTECT", parent_b, 0)
    r.take("THREAD_PROTECT", parent_b, 1)
    r.take("THREAD_READ", parent_b, 2, 0, initial, complement(initial), initial)
    observed(r, parent_a, 0, initial, 7)
    r.take("REFUSE", 1, parent_a, -13, LENGTH, 2)
    r.take("REFUSE", 2, parent_a, -13, LENGTH, 4)
    r.take("REFUSE", 3, parent_a + PAGE, -95, PAGE)
    r.take("REFUSE", 4, parent_a + PAGE, -95, PAGE)
    r.take("REFUSE", 5, parent_a, -95, LENGTH, LENGTH + PAGE, 1)
    observed(r, parent_a, 0, initial, 8)
    r.take("PROTECT", parent_a, LENGTH, 0, 0)
    r.take("COPY_NONE", parent_a + 64 * PAGE, 16, -14, 0, 0, GUARD ^ nonce, (GUARD ^ nonce) ^ MASK)
    r.take("PROTECT", parent_a, LENGTH, 0, 1)
    observed(r, parent_a, 0, initial, 9)
    r.take("TRUNCATE", 10, LENGTH + PAGE, -16, LENGTH)
    r.take("COPY", parent_a + 65 * PAGE, 16, 16, 0, 0, GUARD ^ nonce, (GUARD ^ nonce) ^ MASK)
    observed(r, parent_a, 128 * PAGE, 0, 1)
    r.take("PWRITE", 10, 0, 16, hot, complement(hot), hot)
    observed(r, parent_a, 0, hot, 2)
    observed(r, parent_b, 0, hot, 3)
    r.take("PREAD", 11, 0, 16, hot, complement(hot), hot, GUARD ^ nonce, (GUARD ^ nonce) ^ MASK)
    r.take("CLOSE", 11)
    r.take("DUP", 9, 11, 11)
    grant(r, 11, control_id, 2, CONTROL_BYTES)
    observed(r, parent_a, 0, hot, 4)
    r.take("UNMAP", parent_b, LENGTH)
    r.take("FORK", 1, 0, 3)
    write_before_wake = r.take("PWRITE", 10, 0, 16, woke, complement(woke), woke)
    wake_events = []
    while r.peek("WAKE"):
        e = r.take("WAKE", parent_a + PAGE + 64, 1, ANY, len(wake_events))
        need(signed(e[5]) in (0, 1), "raw wake count")
        wake_events.append(e)
    need(1 <= len(wake_events) <= 128 and [e[5] for e in wake_events] == [0] * (len(wake_events) - 1) + [1],
         "all wake retries and exactly one actual wake")
    wait_exec = r.take("WAITPID", 3, 0, 3)
    observed(r, parent_a, 0, woke, 5)
    r.take("TRUNCATE", 10, LENGTH + PAGE, -16, LENGTH)
    r.take("FORK", 2, 0, 4)
    wait_exit = r.take("WAITPID", 4, 0, 4)
    observed(r, parent_a, 0, woke, 6)
    r.take("UNMAP", parent_a, LENGTH)
    growth = []
    while r.peek("GROW_RELEASE"):
        e = r.take("GROW_RELEASE", 10, LENGTH + PAGE, ANY, ANY)
        need(signed(e[5]) in (0, -16) and e[6] == (LENGTH + PAGE if e[5] == 0 else LENGTH), "scoped EOF lease release")
        growth.append(e)
    need(1 <= len(growth) <= 64 and [signed(e[5]) for e in growth] == [-16] * (len(growth) - 1) + [0],
         "bounded grow release ends in sole success")
    r.take("CLOSE", 10)
    r.take("CLOSE", 11)
    r.take("EXIT_INTENT")
    r.finished()

    c = Events(rows[2])
    grant(c, 9, control_id, 2, CONTROL_BYTES)
    observed(c, parent_a, 0, hot, 10)
    observed(c, parent_a, 127 * PAGE, 0, 11)
    c.take("UNMAP", parent_a, LENGTH)
    e = c.take("GRANT", ANY, ANY, 0, backing[0], backing[1], LENGTH, ANY, 0)
    fd = e[3]
    need(0 <= fd < 256 and fd not in (9, 10, 11) and e[4] & 3 == 0 and e[4] & NO_CACHE and e[9] & 0o170000 == REGULAR,
         "fresh independent real readonly open")
    independent = rows[2][0][17]
    need(independent > 0 and independent != parent_a and independent % PAGE == 0 and independent + LENGTH <= 1 << 47,
         "different VA independent root mapping")
    c.take("MAP", LENGTH, 1, independent, fd, 0, 1, 3, 0x5000000000)
    observed(c, independent, 0, hot, 12)
    c.take("CLOSE", fd)
    ready_event = c.take("READY", independent + PAGE + 64)
    wait_event = c.take("WAIT", independent + PAGE + 64, ANY, 0)
    need(0 < wait_event[4] <= 5_000_000, "actual bounded relative futex timeout")
    observed(c, independent, 0, woke, 13)
    c.take("ACK", independent, 0, 0, woke)
    c.take("CLOSE", 10)
    c.take("CLOSE", 11)
    c.take("EXEC_INTENT", 3)
    c.finished()
    need(write_before_wake[2] <= wake_events[-1][2] <= wait_event[2] and ready_event[2] <= wake_events[-1][2],
         "actual VFS-write/wake/wait ordering")
    a = Events(rows[3])
    a.take("NEW_IMAGE", 3, 1, 0, -9, -9)
    grant(a, 9, control_id, 2, CONTROL_BYTES)
    a.take("EXIT_INTENT")
    a.finished()
    x = Events(rows[4])
    grant(x, 9, control_id, 2, CONTROL_BYTES)
    observed(x, parent_a, 0, woke, 14)
    observed(x, parent_a, 126 * PAGE, 0, 15)
    x.take("EXIT_INTENT", parent_a, LENGTH)
    x.finished()
    host = Events(rows[0])
    host.take("CREATE", 1, 0, physical, 1)
    host.take("REAP", physical, 0, physical)
    host.finished()
    need(wait_exec[2] >= rows[3][0][8] and wait_exit[2] >= rows[4][0][8], "consuming wait follows actual child publication")
    need(len(control) == CONTROL_BYTES, "bounded exact control File")
    req = message(control[:512], nonce, deadline, 0, 1)
    ready = message(control[512:1024], nonce, deadline, 1, 1)
    ack = message(control[1024:1536], nonce, deadline, 2, 2)
    need(req[6:8] == (rows[0][0][5], physical) and ready[6:8] == (3, independent) and ack[6:8] == (3, woke),
         "actual committed request/ready/ack identities")
    need(struct.unpack_from("<4Q", control, 4096) == (GUARD ^ nonce, 0, 0, (GUARD ^ nonce) ^ MASK), "cold usercopy actual guarded bytes")
    for role in range(1, 5):
        offset = 8192 + (role - 1) * WORDS * 8
        need(control[offset:offset + WORDS * 8] == struct.pack(f"<{WORDS}Q", *rows[role][0]), "archive/control exact duplicate")
    for start, end in ((1536, 4096), (4128, 8192), (8192 + 4 * WORDS * 8, CONTROL_BYTES)):
        need(not any(control[start:end]), "control unused zero regions")
    return {"nonce": nonce, "deadline_us": deadline, "physical_create_pid": physical,
            "events": {str(k): len(v[1]) for k, v in rows.items()}, "wake_calls": len(wake_events),
            "grow_calls": len(growth), "scoped_eof_release_observed": True,
            "executor_returned_qualified": False, "global_reference_balance_qualified": False}


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
        if name == b"tmpfs-readshared-probe":
            need((st, rd, pid, pgid, gen, restart, wait, error) == (6, 2, -1, -1, 1, 0, 0, 0), "exact probe exited successfully")
            need(any(h[2:4] == (6, 5) for h in history), "actual init EXIT transition")
            targets.append(name)
    need(len(targets) == 1 and not any(data[64 + count * 256:]), "one target and zero unused services")
    expected = (f"PASS tmpfs-readshared-init-exit service=tmpfs-readshared-probe generation=1 restarts=0 sequence={sequence} "
                f"published-ns={published} state=6 readiness=2 wait=0 error=0 history-exit=1 snapshot-bytes=4160 abi=1 read-error=0 failures=0\n")
    padded_line(status, expected)
    return {"sequence": sequence, "published_ns": published, "probe_reaped_by_init": True}


def padded_line(data, expected):
    need(len(data) == 512 and data == expected.encode() + bytes(512 - len(expected)), "exact padded status line")


def validate(directory):
    directory = directory.resolve(strict=True)
    expected = {"control.bin": CONTROL_BYTES, "host.bin": WORDS * 8, "status": 512,
                "init-exit-status": 512, "init-exit-snapshot.bin": 4160,
                **{f"role-{r}.bin": WORDS * 8 for r in range(1, 5)}}
    captured = {}
    pins = []
    for name, size in expected.items():
        p = directory / name
        need(not p.is_symlink() and p.is_file() and p.stat().st_size == size, "exact regular saved input: " + name)
        b = p.read_bytes()
        need(len(b) == size, "saved read extent held")
        captured[name] = b
        pins.append({"path": str(p), "bytes": len(b), "sha256": hashlib.sha256(b).hexdigest()})
    rows = {0: decoded_row(captured["host.bin"], 0),
            **{r: decoded_row(captured[f"role-{r}.bin"], r) for r in range(1, 5)}}
    result = validate_rows(rows, captured["control.bin"])
    padded_line(captured["status"], f"PUBLIC_COMPLETE tmpfs-readshared nonce={result['nonce']} failures=0\n")
    result["init"] = init_snapshot(captured["init-exit-snapshot.bin"], captured["init-exit-status"])
    for pin in pins:
        b = Path(pin["path"]).read_bytes()
        need(len(b) == pin["bytes"] and hashlib.sha256(b).hexdigest() == pin["sha256"], "saved inputs held after decoding")
    result.update(qualification="PASS_SAVED_PUBLIC_TMPFS_READSHARED_PROTOCOL_ONLY", input_pins=pins,
                  source_artifact_identity_qualified=False, vm_identity_qualified=False, full_goal_complete=False)
    return result


def selftest():
    # Parser tests are explicitly synthetic and never written as guest receipts.
    nonce = (1 << 63) | 3
    row = [0] * WORDS
    row[:12] = [MAGIC, 1, 1, nonce, 60_000_001, 1, 0, 10, 20, 1, 0, 0]
    row[20:27] = [100, 100, 100, 100, 1, 0, 256]
    row[29:31] = [WORDS * 8, CAPACITY]
    row[62:64] = [GUARD ^ nonce ^ 1, (GUARD ^ nonce ^ 1) ^ MASK]
    row[HEAD:HEAD + EW] = [OP["READ"], 1, 15, 4096, 0, 0, 7, complement(7), 7, 1, 0, GUARD ^ nonce ^ 1 ^ 1]
    row[-1] = nonce
    pack = lambda value: struct.pack(f"<{WORDS}Q", *value)
    decoded = decoded_row(pack(row), 1)
    observed(Events(decoded), 4096, 0, 7, 1)
    rejected = 0
    for index in [0, 1, 2, 3, 4, 7, 8, 9, 10, 11, 20, 24, 25, 26, 29, 30, 31, 33, 62, 63, HEAD, HEAD + 1, HEAD + 2, HEAD + 11, HEAD + EW, WORDS - 1]:
        altered = row.copy()
        altered[index] ^= 0x10000
        if index in (4, 8):
            altered[index] = 0
        try:
            decoded_row(pack(altered), 1)
        except ValueError:
            rejected += 1
        else:
            raise ValueError(f"synthetic malformed field accepted: {index}")
    altered = row.copy()
    altered[HEAD + 6] ^= 1
    try:
        observed(Events(decoded_row(pack(altered), 1)), 4096, 0, 7, 1)
    except ValueError:
        rejected += 1
    else:
        raise ValueError("synthetic wrong actual payload accepted")
    need(rejected == 27, "all malformed synthetic vectors rejected")
    return {"qualification": "PASS_DECODER_SYNTHETIC_UNIT_VECTORS_ONLY", "negative_vectors": rejected,
            "guest_execution_qualified": False, "vm_identity_qualified": False, "full_goal_complete": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", nargs="?", type=Path)
    parser.add_argument("--selftest", action="store_true")
    args = parser.parse_args()
    try:
        need(bool(args.directory) != args.selftest, "choose saved directory or synthetic selftest")
        result = selftest() if args.selftest else validate(args.directory)
    except (OSError, ValueError, struct.error) as error:
        print(json.dumps({"qualification": "FAIL", "errors": [str(error)], "full_goal_complete": False}))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
