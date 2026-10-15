"""Strict independent read-only decoder of the reviewed two-hour public wire.

No producer import, helper execution, process action or write occurs here. Saved
guest work, init consumption and externally received Host time remain separate.
"""
import hashlib
import os
from pathlib import Path
import re
import stat
import struct

MASK = (1 << 64) - 1
MAGIC = 0x5733393248435031
VERSION = 1
GUARD = 0xA596C387E1D24B6F
SKIP = -(1 << 63)
SENTINEL = 0xA5A5A5A5FFFFFFFF
CONSUMED = 0xA5A5A5A500000000
ROUNDS = 1443
INTERVAL_US = 5_000_000
TOLERANCE_US = 2_000_000
OVERLAP_US = 7_200_000_000
GUEST_US = 7_440_000_000
ROOT_BYTES = 33_554_432
EVIDENCE_BOUNDS = {
    'control-a.bin': 786432, 'control-b.bin': 786432,
    'host-ledger.bin': 763392, 'host-operational': 64,
    'marker-a': 64, 'marker-b': 64,
    'cli-stdout.bin': 8192, 'cli-stderr.bin': 8192,
    'proc-initial.bin': 16384, 'proc-final.bin': 16384,
    'init-exit-snapshot.bin': 4160, 'init-exit-status': 512,
    'campaign-status': 512,
}
EVIDENCE_NAMES = tuple(EVIDENCE_BOUNDS)
EVIDENCE_CAP = 2390784
VARIABLE_NAMES = frozenset()
SERVICES = ('journald', 'twohour-controller', 'twohour-exit-witness')
RETAINED_NAMES = EVIDENCE_NAMES[:8]


def require(value, message):
    if not value:
        raise ValueError(message)


def signed(value):
    require(type(value) is int and 0 <= value <= MASK, 'uint64 raw word')
    return value - (1 << 64) if value >> 63 else value


def bits(value):
    return value & MASK


def words(data, count):
    require(type(data) is bytes and len(data) == count * 8, 'exact LE64 word buffer')
    return struct.unpack('<' + 'Q' * count, data)


def guard(nonce, tree, role, selector):
    return (GUARD ^ nonce ^ (tree << 48) ^ (role << 32) ^ selector) & MASK


def commit(nonce, role, round_number, sequence):
    return (MAGIC ^ nonce ^ (role << 32) ^ round_number ^ sequence) & MASK


def raw_hash(data):
    value = 1469598103934665603
    for byte in data:
        value = ((value ^ byte) * 1099511628211) & MASK
    return value


def encoded(row):
    return struct.pack('<' + 'Q' * len(row), *row)


def cpu_result(nonce, role, round_number):
    value = (nonce ^ (role << 32) ^ round_number) | 1
    for i in range(256):
        value = ((value * 6364136223846793005 + 1442695040888963407) & MASK) ^ i
    return value


def marker_token(nonce, tree):
    return (MAGIC, VERSION, nonce, tree,
            0x4141414141414141 if tree == 1 else 0x4242424242424242,
            guard(nonce, tree, 0, 11), 0, commit(nonce, 0, 0, 1))


def operation_token(nonce, round_number):
    return (MAGIC, VERSION, nonce, 5, round_number, round_number + 1,
            guard(nonce, 0, 5, 10 + round_number),
            commit(nonce, 5, round_number, round_number + 1))


def identity(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_nlink, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns)


def read_evidence(files):
    require(type(files) is dict and set(files) == set(EVIDENCE_NAMES), 'exact thirteen evidence names')
    require(sum(EVIDENCE_BOUNDS.values()) == EVIDENCE_CAP, 'exact durable2390784 bound')
    saved, pins, identities = {}, [], set()
    for name, size in EVIDENCE_BOUNDS.items():
        path = Path(files[name])
        before = path.lstat()
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size == size,
                name + ': exact regular nlink1 file')
        expected = identity(before)
        require(expected[:2] not in identities, name + ': distinct saved file identity')
        identities.add(expected[:2])
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            require(identity(os.fstat(fd)) == expected, name + ': opened exact identity')
            copies = []
            for _ in range(2):
                os.lseek(fd, 0, os.SEEK_SET)
                data = bytearray()
                while len(data) <= size:
                    chunk = os.read(fd, min(65536, size + 1 - len(data)))
                    if not chunk:
                        break
                    data.extend(chunk)
                require(len(data) == size and identity(os.fstat(fd)) == expected,
                        name + ': complete bounded unchanged read')
                copies.append(bytes(data))
            require(copies[0] == copies[1] and identity(path.lstat()) == expected,
                    name + ': whole double read and path equality')
        finally:
            os.close(fd)
        saved[name] = copies[0]
        pins.append({'path': str(path.absolute()), 'name': name, 'bytes': size,
                     'sha256': hashlib.sha256(copies[0]).hexdigest()})
    return saved, pins


def padded_line(data, label):
    require(type(data) is bytes and len(data) == 512 and b'\0' in data, label + ': padded512')
    line, tail = data.split(b'\0', 1)
    require(line and line.endswith(b'\n') and line.count(b'\n') == 1 and not any(tail),
            label + ': one final-newline prefix and zero padding')
    return line.decode('ascii')


def spawns(serial):
    require(type(serial) is str and len(serial.encode()) <= 64 * 2**20, 'bounded actual own serial')
    result = {}
    for name, priority in zip(SERVICES, (10, 0, 0)):
        matches = re.findall(r'init supervisor: spawned ' + re.escape(name) +
                             r' pid=([1-9][0-9]*) pgid=([1-9][0-9]*) generation=([0-9]+) '
                             r'priority=(-?[0-9]+)(?:\s|$)', serial)
        require(len(matches) == 1, name + ': exactly one original service spawn')
        pid, pgid, generation, actual_priority = map(int, matches[0])
        require(pid == pgid and pid > 2 and generation == 1 and actual_priority == priority,
                name + ': original physical PID/PGID/generation/priority')
        result[name] = pid
    require(len(set(result.values())) == 3, 'three distinct service processes')
    return result


def stat_fields(raw, regular, size=None, expected_identity=None):
    require(len(raw) == 144, 'full144 raw stat bytes')
    f = struct.unpack('<3Q4IQ12q', raw)
    mode = 0o100600 if regular else 0o20666
    require(f[1] > 0 and f[2] == 1 and f[3] == mode and
            f[4:6] == ((100, 100) if regular else (0, 0)) and f[6] == f[7] == 0 and
            f[9] == 4096 and f[10] >= 0 and f[10] % 8 == 0 and f[17:] == (0, 0, 0),
            'complete stat mode/ownership/nlink/padding/rdev/block/unused fields')
    require(all(f[i] >= 0 and 0 <= f[i + 1] < 1_000_000_000 for i in (11, 13, 15)),
            'all stat timestamps valid')
    if regular:
        require(f[8] == size and (expected_identity is None or f[:2] == expected_identity),
                'regular exact size and retained File identity')
    else:
        require(f[8] == f[10] == 0, 'console CHR zero size/blocks')
    return f[:2]


def limits(row, nonce, tree, role):
    for i in range(16):
        g = guard(nonce, tree, role, 0x100 + i)
        wanted = 256 if i == 7 else MASK
        require(row[32 + 6 * i:38 + 6 * i] == (0, g, wanted, wanted, g ^ MASK, i),
                f'role{role}: guarded actual GETRLIMIT pair{i}')


def attestation(raw, nonce, role, phase, deadline, host_pid, host_parent, requests,
                physical, local, creator, host_fds=None, host_identities=None):
    row = words(raw, 512)
    tree = (role + 1) // 2 if role < 5 else 0
    pid, parent = (host_pid, host_parent) if role == 5 else (local[role - 1], 0 if role % 2 else 1)
    require(row[:8] == (MAGIC, VERSION, nonce, role, phase, tree, pid, parent) and
            row[8:15] == (100, 100, 100, 100, 1, 0, 256), 'entry/final identity and policy')
    require(0 < row[15] == row[25] <= row[26] < deadline and row[28:30] == (0, 0) and
            row[30] == guard(nonce, tree, role, 5 + phase) and
            row[31] == row[511] == commit(nonce, role, 0, phase + 1),
            'attestation time/zero failures/full commits')
    limits(row, nonce, tree, role)
    if role < 5:
        request = requests[tree - 1]
        require(row[16:24] == (ROOT_BYTES, request[15], request[16], 786432, 2,
                              request[13], 64, bits(-2)) and
                row[24] == (local[role] if role % 2 else 0) and row[27] == 9 and
                not any(row[480:511]), 'private entry/final resources/marker/fork/unused tail')
        descriptors = (0, 1, 2, 9)
        require(all(signed(row[128 + fd]) == (0 if fd == 9 else -9) for fd in range(256)),
                'private full256 signed descriptor census only FD9')
    else:
        require(row[16:25] == (creator[0], creator[1], physical[2], physical[1], physical[3],
                              9, 64, 64, 0) and row[27] == 8 and
                row[480:488] == tuple(host_fds) and row[488:496] == (bits(SKIP),) * 8 and
                not any(row[496:511]), 'Host entry/final originals/nineFD/eightowned/unused tail')
        descriptors = host_fds[:4]
        require(all(signed(row[128 + fd]) == (0 if fd == 2 else 1 if fd in host_fds else -9)
                    for fd in range(256)), 'Host exact full256 signed descriptor census')
    observed = []
    for slot, fd in enumerate(descriptors):
        at = 384 + slot * 24
        g = guard(nonce, tree, role, 0x200 + fd)
        require(row[at] == fd and row[at + 4] == g and row[at + 23] == (g ^ MASK),
                'full stat descriptor and both guards')
        raw_stat = encoded(row[at + 5:at + 23])
        if role < 5 and fd != 9:
            require(tuple(map(signed, row[at + 1:at + 4])) == (-9, -9, -9) and raw_stat == b'\xa5' * 144,
                    'absent stdio actual EBADF and untouched full144 output')
        else:
            require(row[at + 1] == (0 if role < 5 else 1) and signed(row[at + 2]) >= 0 and
                    row[at + 2] & 3 == 2 and row[at + 3] == 0, 'actual present stat/getfd/getfl RW')
            size = 786432 if role < 5 or slot < 2 else 763392 if slot == 2 else 64
            expected = (requests[tree - 1][15], requests[tree - 1][16]) if role < 5 else (
                host_identities[slot] if host_identities else None)
            observed.append(stat_fields(raw_stat, True, size, expected))
    return row, observed


def private_request(raw, nonce, tree, host_pid, deadline, host_limits, marker):
    r = words(raw, 64)
    require(r[:15] == (MAGIC, VERSION, nonce, tree, host_pid, deadline, 100, 100, 100, 100,
                       1, 0, 256, raw_hash(marker), 64) and r[16] > 0 and
            r[17:27] == (786432, 2, tree, ROUNDS, INTERVAL_US, OVERLAP_US, TOLERANCE_US,
                         ROOT_BYTES, 512, r[26]) and r[26] > 0 and r[28:30] == (0, 0) and
            r[30] == guard(nonce, tree, 0, 0) and r[31] == commit(nonce, 0, 0, 1) and
            r[32:] == host_limits, 'complete private immutable request and limits commit31')
    return r


def private_sample(raw, nonce, role, round_number, target, deadline, local, marker_hash):
    row = words(raw, 32)
    tree = (role + 1) // 2
    g = guard(nonce, tree, role, 0x300)
    require(row[:6] == (MAGIC, VERSION, nonce, role, round_number, round_number + 1) and
            target <= row[6] <= row[7] < min(deadline, target + TOLERANCE_US) and
            row[8:17] == (local, 0 if role % 2 else 1, 100, 100, 100, 100, 1, 0, 256),
            'private sample original identity/work time/policy')
    require(0 <= signed(row[17]) < 256 and row[17] != 9 and row[18:22] == (64, 0, marker_hash, 256) and
            row[22] == cpu_result(nonce, role, round_number) and
            signed(row[23]) == (0 if role % 2 else SKIP) and
            row[24:30] == (g, SENTINEL, g ^ MASK, 0, target, 0) and
            row[30] == guard(nonce, tree, role, 3 + round_number) and
            row[31] == commit(nonce, role, round_number, round_number + 1),
            'private actual marker/CPU/wait/guards/reserved/commit')
    return row


def host_sample(raw, nonce, round_number, target, deadline, host_pid, host_parent,
                physical, creator, private_rows):
    row = words(raw, 64)
    expected_hash = raw_hash(encoded(operation_token(nonce, round_number)))
    require(row[:6] == (MAGIC, VERSION, nonce, 5, round_number, round_number + 1) and
            target <= row[6] <= row[7] < min(deadline, target + TOLERANCE_US) and
            row[8:20] == (host_pid, host_parent, 100, 100, 100, 100, target, 64, 64, 0,
                           expected_hash, expected_hash), 'Host real work/time/original IDs')
    require(row[20:24] == (0,) * 4 and row[44:48] == tuple(physical) and
            0 <= signed(row[48]) < 256 and row[49:52] == (64, 0, expected_hash) and
            not any(row[52:61]) and row[61] == guard(nonce, 0, 5, 4 + round_number) and
            row[62] == 0 and row[63] == commit(nonce, 5, round_number, round_number + 1),
            'Host signal0/freshpath/rawread/unused/commit')
    for i, at in enumerate((24, 30)):
        g = guard(nonce, 0, 5, 0x300 + i)
        require(row[at:at + 6] == (creator[i], 0, g, SENTINEL, g ^ MASK, 1),
                'Host two actual original nohang0 guarded waits')
    require(row[36:44] == tuple(value for p in private_rows for value in (p[5], p[7])) and
            all(p[7] <= row[7] for p in private_rows), 'committed private sample references/time')
    return row


def proc_slot(raw, length, expected_hash, pid, parent):
    require(len(raw) == 4096 and 0 < length <= 4095 and raw[length - 1:length] == b'\n' and
            b'\0' not in raw[:length] and not any(raw[length:]) and raw_hash(raw) == expected_hash,
            'full padded actual proc capture and hash')
    text = raw[:length].decode('ascii')
    require(text.startswith('Name:\tprobe\n'), 'original probe proc name')
    fields = {}
    for line in text.splitlines():
        if ':' in line:
            key, value = line.split(':', 1)
            require(key not in fields, 'no duplicate proc field')
            fields[key] = value.strip()
    require(fields.get('Pid') == fields.get('Tgid') == str(pid) and
            fields.get('PPid') == str(parent) and fields.get('Uid', '').split() == ['100'] * 4,
            'original physical root/worker identity from real public proc')
    return fields


def cli_manifest(raw, nonce, private_pid, limits_row):
    require(len(raw) == 8192 and b'\0' in raw, 'full8192 CLI output')
    prefix, tail = raw.split(b'\0', 1)
    require(prefix.endswith(b'\n') and not any(tail) and len(prefix) <= 8191, 'bounded CLI prefix/zero padding')
    lines = prefix.decode('ascii').splitlines()
    require(len(lines) == 23 and all(0 < len(line) <= 511 for line in lines), 'exact23 bounded CLI lines')
    require(lines[0] == f'compartment spec=1 host-pid={private_pid} exec=/app/probe network=denied ceiling=0x0 root-bytes={ROOT_BYTES}',
            'real CLI current spec/original B root/denied network')
    sources = ('/usr/libexec/goal39-twohour-runtime', '/lib/ld.so', '/usr/lib/libc.so', '/srv/twohour/marker-b')
    destinations = ('/app/probe', '/lib/ld.so', '/usr/lib/libc.so', '/same/name')
    fds = []
    for i in range(4):
        m = re.fullmatch(r'asset fd=([0-9]+) source=' + re.escape(sources[i]) +
                         ' destination=' + re.escape(destinations[i]) + r' mode=(755|644) copied', lines[i + 1])
        require(m is not None, 'exact CLI imported asset line')
        fd, mode = map(int, m.groups())
        require(0 <= fd < 256 and fd not in (1, 2, 9) and fd not in fds and mode == (755 if i < 2 else 644),
                'CLI actual distinct safe asset FDs and exact modes')
        fds.append(fd)
    require(lines[5:7] == ['grant source-fd=9 destination-fd=9 shared-file rights-and-offset',
                           'arguments=6 environment=1 dumpable=0 service-privileges=0x0'], 'CLI exact grant/vector request')
    for i in range(16):
        require(lines[7 + i] == f'limit id={i} soft={limits_row[2 * i]} hard={limits_row[2 * i + 1]}',
                'CLI inherited limits exact')
    return len(prefix), fds


def check_host_receive(receipt, nonce, serial_bytes, sequence, s, e):
    require(type(receipt) is dict and receipt.get('schema') == 1 and receipt.get('errors') == [] and
            receipt.get('actual_monotonic_clock') is True and receipt.get('complete') is True and
            type(receipt.get('max_poll_gap_ns')) is int and 0 <= receipt['max_poll_gap_ns'] <= 1_000_000_000 and
            type(receipt.get('max_buffer_bytes')) is int and 0 <= receipt['max_buffer_bytes'] <= 65536 and
            receipt.get('serial_bytes') == len(serial_bytes) and
            receipt.get('serial_sha256') == hashlib.sha256(serial_bytes).hexdigest(),
            'independent actual bounded complete Host receive ledger')
    rows = receipt.get('markers')
    expected = [f'WOS39_2H_START nonce={nonce:016x} S={s} rounds=1443 roles=5\n',
                f'WOS39_2H_END nonce={nonce:016x} E={e} rounds=1443 roles=5\n',
                f'WOS39_2H_CONTROLLER_COMMITTED nonce={nonce:016x} sequence=1\n',
                f'WOS39_2H_WITNESS_COMMITTED nonce={nonce:016x} sequence={sequence}\n']
    require(re.findall(rb'WOS39_2H_[^\n]*\n', serial_bytes) == [line.encode() for line in expected],
            'whole saved serial has exactly four expected markers, no malformed/wrong-nonce/duplicate extra')
    require(type(rows) is list and len(rows) == 4, 'exact four distinct ordered received success markers')
    previous_offset = previous_ns = -1
    for row, line in zip(rows, expected):
        raw = line.encode('ascii')
        offset, received = row.get('offset'), row.get('received_mono_ns')
        require(type(offset) is int and type(received) is int and offset >= 0 and received > 0 and offset > previous_offset and received >= previous_ns and
                row.get('line_ascii') == line and row.get('bytes') == len(raw) and row.get('nonce') == nonce and
                row.get('sha256') == hashlib.sha256(raw).hexdigest() and
                serial_bytes[offset:offset + len(raw)] == raw and serial_bytes.count(raw) == 1 and
                row.get('scan_started_mono_ns') <= received <= row.get('scan_completed_mono_ns'),
                'actual complete first-read newline bytes/offset/hash/nonce/time binding')
        previous_offset, previous_ns = offset, received
    delta = rows[1]['received_mono_ns'] - rows[0]['received_mono_ns']
    require(delta >= OVERLAP_US * 1000 and receipt.get('receive_delta_ns') == delta,
            'independent actual START/END receive interval at least7200 seconds')
    return {'receive_delta_ns': delta, 'markers': rows, 'qualification': 'PASS_ACTUAL_EXTERNAL_HOST_RECEIVE_INTERVAL_ONLY'}


def decode_saved_bytes(saved, serial):
    require(type(saved) is dict and set(saved) == set(EVIDENCE_NAMES), 'exact13 complete saved buffers')
    require(all(type(saved[n]) is bytes and len(saved[n]) == size for n, size in EVIDENCE_BOUNDS.items()),
            'each actual durable file exact fixed size')
    host = saved['host-ledger.bin']
    request = words(host[:512], 64)
    nonce, host_pid, host_parent, deadline, start = request[2], request[4], request[5], request[6], request[8]
    require(nonce > 0 and nonce & 1 and host_pid > 2 and host_parent == 1 and
            0 < start <= MASK - GUEST_US and deadline == start + GUEST_US and
            nonce == ((start ^ ((host_pid << 32) & MASK) ^ GUARD) | 1), 'one-clock generated nonce/original init child/deadline')
    require(request[:4] == (MAGIC, VERSION, nonce, 5) and request[7] == OVERLAP_US and
            request[9:28] == (1, 0, 256, 100, 100, 100, 100, 1, 408, 786432, 763392,
                             ROUNDS, INTERVAL_US, OVERLAP_US, TOLERANCE_US, 20 * 2**30, 40 * 2**30, 3 * 2**20, 3) and
            request[28:30] == (0, 0) and request[30] == guard(nonce, 0, 5, 0) and
            request[31] == commit(nonce, 5, 0, 1), 'complete Host request spec/geometry/budget/guard commit31')
    require(request[32:] == tuple(v for i in range(16) for v in ((256 if i == 7 else MASK),) * 2), 'all16 inherited limits')
    controls = [saved['control-a.bin'], saved['control-b.bin']]
    requests = []
    for i, control in enumerate(controls):
        marker = saved['marker-a' if i == 0 else 'marker-b']
        require(words(marker, 8) == marker_token(nonce, i + 1), 'tree-specific same-path marker complete bytes')
        requests.append(private_request(control[:512], nonce, i + 1, host_pid, deadline, request[32:], marker))
        require(not any(control[2560:4096]) and not any(control[21504:24576]) and
                not any(control[763392:]), 'all private reserved/tail bytes zero')
    require(not any(host[1024:4096]) and not any(host[16384:24576]), 'all Host reserved bytes zero')
    summary = words(host[12288:16384], 512)
    creator = (summary[16], summary[17])
    physical = (summary[10], summary[12], summary[11], summary[13])
    require(min(*physical, *creator) > 2 and len(set((*physical, host_pid, creator[1]))) == 6 and
            creator[0] == physical[0] and summary[9] == creator[1], 'original positive CREATE/FORK/four physical roots and workers')
    require(summary[:5] == (MAGIC, VERSION, nonce, 5, 1) and summary[7] == start and
            summary[14] == ROUNDS and summary[30] == guard(nonce, 0, 5, 8) and
            summary[31] == summary[511] == commit(nonce, 5, 0, 2) and
            not any(summary[57:64]) and not any(summary[85:511]), 'complete summary header/reserved/full commit')
    local = (1, words(controls[0][4096:8192], 512)[24], 1, words(controls[1][4096:8192], 512)[24])
    require(local[1] > 1 and local[3] > 1, 'original private positive worker forks')
    initial_host = words(host[4096:8192], 512)
    host_fds = initial_host[480:488]
    require(all(0 <= fd < 256 and fd != 2 for fd in host_fds) and len(set(host_fds)) == 8,
            'eight actual distinct owned Host descriptor numbers')
    initial_h, identities = attestation(host[4096:8192], nonce, 5, 0, deadline, host_pid, host_parent,
                                         requests, physical, local, creator, host_fds)
    require(len(set(identities)) == 4 and identities[:2] == [(r[15], r[16]) for r in requests],
            'distinct retained regular identities and exact private grant/control match')
    final_h, _ = attestation(host[8192:12288], nonce, 5, 1, deadline, host_pid, host_parent,
                              requests, physical, local, creator, host_fds, identities)
    require(initial_h[128:384] == final_h[128:384], 'Host initial/final fullFD census unchanged')
    attestations = {5: (initial_h, final_h)}
    for i in range(4):
        control, role = controls[i // 2], i + 1
        offset = 4096 + (i % 2) * 4096
        initial, _ = attestation(control[offset:offset + 4096], nonce, role, 0, deadline, host_pid, host_parent,
                                 requests, physical, local, creator)
        final, _ = attestation(control[offset + 8192:offset + 12288], nonce, role, 1, deadline, host_pid, host_parent,
                               requests, physical, local, creator)
        require(initial[128:384] == final[128:384] and initial[26] < final[15] and
                summary[64 + i] == raw_hash(encoded(initial)) and summary[68 + i] == raw_hash(encoded(final)),
                'private initial/final census/chronology and full summary hashes')
        attestations[role] = (initial, final)
    anchor = words(host[512:1024], 64)
    s, e, target0 = summary[5], summary[6], anchor[6]
    require(s <= MASK - (ROUNDS - 1) * INTERVAL_US and target0 <= MASK - TOLERANCE_US and
            start < target0 and all(pair[0][26] < target0 for pair in attestations.values()) and
            0 < s <= e < deadline and e - s == summary[15] >= OVERLAP_US and
            e < final_h[15] <= final_h[26] <= summary[8] < deadline,
            'actual common >=7200s and complete campaign chronology, checked schedule arithmetic')
    samples = {role: [] for role in range(1, 6)}
    previous = [attestations[role][0][26] for role in range(1, 6)]
    for round_number in range(ROUNDS):
        target = target0 if round_number == 0 else s + round_number * INTERVAL_US
        require(target <= MASK - TOLERANCE_US and target < deadline, 'checked actual serialized target')
        private_rows = []
        for i in range(4):
            offset = (24576 if i % 2 == 0 else 393984) + round_number * 256
            p = private_sample(controls[i // 2][offset:offset + 256], nonce, i + 1, round_number,
                               target, deadline, local[i], requests[i // 2][13])
            private_rows.append(p)
        offset = 24576 + round_number * 512
        h = host_sample(host[offset:offset + 512], nonce, round_number, target, deadline,
                        host_pid, host_parent, physical, creator, private_rows)
        for i, sample in enumerate((*private_rows, h)):
            require(previous[i] <= sample[6], 'each original role consecutive work chronology')
            previous[i] = sample[7]
            samples[i + 1].append(sample)
    first, last = [samples[role][0] for role in range(1, 6)], [samples[role][-1] for role in range(1, 6)]
    require(s == max(row[7] for row in first) and e == min(row[6] for row in last),
            'S=max all five actual first ends; E=min all five actual last starts')
    require(anchor[:7] == (MAGIC, VERSION, nonce, 5, 1, s, target0) and
            max(row[7] for row in first) <= anchor[7] < deadline and
            anchor[8:13] == tuple(row[7] for row in first) and
            anchor[13:18] == tuple(raw_hash(encoded(row)) for row in first) and
            anchor[18:20] == (2, 0) and signed(anchor[20]) >= 0 and anchor[20] & 3 == 1 and
            anchor[21] == 0 and anchor[22] == guard(nonce, 0, 5, 0x202) and
            anchor[41] == (anchor[22] ^ MASK) and anchor[45] == 8 and
            not any(anchor[46:62]) and anchor[62] == guard(nonce, 0, 5, 9) and
            anchor[63] == commit(nonce, 5, 0, 1), 'full actual console/time anchor geometry')
    stat_fields(encoded(anchor[23:41]), False)
    start_line = f'WOS39_2H_START nonce={nonce:016x} S={s} rounds=1443 roles=5\n'
    end_line = f'WOS39_2H_END nonce={nonce:016x} E={e} rounds=1443 roles=5\n'
    require(anchor[42:45] == (len(start_line), len(start_line), raw_hash(start_line.encode())) and
            summary[52:57] == (2, len(end_line), len(end_line), raw_hash(start_line.encode()), raw_hash(end_line.encode())),
            'actual raw serial write lengths and exact bytes hashes')
    for i in range(4):
        role, tree, control = i + 1, i // 2 + 1, controls[i // 2]
        command = words(control[512 + (i % 2) * 512:1024 + (i % 2) * 512], 64)
        require(command[:12] == (MAGIC, VERSION, nonce, tree, role, 1444, 3, 1443, 0, s, deadline, requests[i // 2][30]) and
                not any(command[12:62]) and command[62] == guard(nonce, tree, role, 1444) and
                command[63] == commit(nonce, role, 1443, 1444), 'complete final FINISH command')
        progress = words(control[1536 + (i % 2) * 512:2048 + (i % 2) * 512], 64)
        require(progress[:32] == samples[role][-1] and not any(progress[32:63]) and
                progress[63] == commit(nonce, role, 1442, ROUNDS), 'exact final progress copy/zero reserves/commit')
        offset = 20480 + (i % 2) * 512
        lifecycle = words(control[offset:offset + 512], 64)
        root = i % 2 == 0
        g = guard(nonce, tree, role, 0x300)
        require(lifecycle[:11] == (MAGIC, VERSION, nonce, role, tree, local[i + 1] if root else 0,
                                   local[i + 1] if root else bits(SKIP), g,
                                   CONSUMED if root else SENTINEL, g ^ MASK, 1) and
                samples[role][-1][7] <= lifecycle[11] <= lifecycle[12] <= attestations[role][1][15] and
                lifecycle[12] < deadline and lifecycle[12] - lifecycle[11] < 10_000_000 and
                lifecycle[13:16] == (0, 0, 0) and not any(lifecycle[16:63]) and
                lifecycle[63] == commit(nonce, role, 0, 2), 'private exact original consumingwait/lifecycle/full reserves')
    for i, at in enumerate((32, 40)):
        g = guard(nonce, 0, 5, 0x300 + i)
        require(summary[at:at + 6] == (creator[i], creator[i], g, CONSUMED, g ^ MASK, 1) and
                e <= summary[at + 6] <= summary[at + 7] < deadline and
                summary[at + 7] - summary[at + 6] < 10_000_000 and
                summary[at + 7] <= final_h[15], 'Host actual two exact original consuming waits/status0/bounded time')
    require(summary[39] <= summary[46], 'Host A consuming wait precedes CLI consuming wait')
    require(summary[50:52] == (8192, 8192) and not any(saved['cli-stderr.bin']), 'actual two complete CLI raw reads and zero stderr')
    length, cli_fds = cli_manifest(saved['cli-stdout.bin'], nonce, physical[2], request[32:])
    require(summary[18:22] == (length, 0, raw_hash(saved['cli-stdout.bin']), raw_hash(saved['cli-stderr.bin'])) and
            summary[80:85] == (ROUNDS,) * 5 and words(saved['host-operational'], 8) == operation_token(nonce, ROUNDS - 1),
            'complete CLI hashes/counts, every five ledger count and final operational bytes')
    proc_fields = {}
    for phase in range(2):
        raw = saved['proc-initial.bin' if phase == 0 else 'proc-final.bin']
        for i in range(4):
            parent = host_pid if i == 0 else creator[1] if i == 2 else physical[i - 1]
            proc_fields[f'{phase}:{i + 1}'] = proc_slot(raw[i * 4096:(i + 1) * 4096],
                summary[22 + phase * 4 + i], summary[72 + phase * 4 + i], physical[i], parent)
    actual_spawns = spawns(serial)
    require(actual_spawns['twohour-controller'] == host_pid and
            serial.count(start_line) == serial.count(end_line) == 1 and
            serial.index(start_line) < serial.index(end_line), 'original init spawn and unique ordered actual serial clock lines')
    status = padded_line(saved['campaign-status'], 'campaign status')
    require(status == f'PASS twohour-runtime nonce={nonce} host={host_pid} rounds=1443 S={s} E={e} overlap-us={e-s} failures=0\n',
            'successful campaign text independently corroborates complete decoded wire')
    return {'qualification': 'PASS_SAVED_TWOHOUR_GUEST_WIRE_ONLY', 'errors': [], 'nonce': nonce,
            'supervisor_pid': host_pid, 'deadline_us': deadline, 'S_us': s, 'E_us': e,
            'guest_overlap_us': e - s, 'physical_roles': list(physical), 'local_roles': list(local),
            'actual_service_spawns': actual_spawns, 'cli_asset_fds': cli_fds, 'proc_fields': proc_fields,
            'five_role_samples': ROUNDS * 5, 'attestations': 10, 'signed_fd_results': 2560,
            'guarded_limit_pairs': 160, 'complete_stat_slots': 40, 'console_stat_slots': 1,
            'actual_durable_bytes': EVIDENCE_CAP, 'runtime_qualified': False,
            'criterion8_qualified': False, 'full_goal_qualified': False,
            'limits': ['Host external receive interval and original init consumption are separate mandatory gates.',
                       'CLI owns B root foreground consuming wait; no saved Host B consuming wait is invented.',
                       'No mapping/WKI/1000cycle/fullGC or complete CLI parser/help qualification.']}


def decode_init_saved_bytes(saved, probe, serial, pins=None):
    require(probe.get('qualification') == 'PASS_SAVED_TWOHOUR_GUEST_WIRE_ONLY' and probe.get('errors') == [] and
            (pins is None or probe.get('source_pins') == pins), 'same independently decoded complete public bytes')
    nonce = probe['nonce']
    status = padded_line(saved['init-exit-status'], 'init witness status')
    match = re.fullmatch(r'PASS twohour-init-exit nonce=' + str(nonce) +
        r' service=twohour-controller generation=1 restarts=0 sequence=([1-9][0-9]*) state=6 readiness=1 '
        r'wait=0 error=0 history-exit=1 snapshot-bytes=4160 abi=1 read-error=0 failures=0\n', status)
    require(match is not None, 'exact successful NOT_READY1 original daemon witness text')
    sequence = int(match[1])
    raw = saved['init-exit-snapshot.bin']
    header = struct.unpack_from('<4I2Q2I3Q', raw)
    require(header[:4] == (4160, 1, 2, 0) and header[4] == sequence > 0 and header[5] > 0 and
            header[6] == 3 and header[7:] == (0,) * 4, 'raw RUNNING exact init header/sequence/reserved/services')
    services, histories = {}, {}
    actual_spawns = spawns(serial)
    for i in range(3):
        offset = 64 + i * 256
        f = struct.unpack_from('<64s4I2q2Q2i2Q2I2Q', raw, offset)
        require(b'\0' in f[0], 'bounded init raw service name')
        name, tail = f[0].split(b'\0', 1)
        name = name.decode('ascii')
        require(name in SERVICES and name not in services and not any(tail) and 1 <= f[1] <= 8 and
                1 <= f[2] <= 3 and f[3] & ~7 == 0 and f[4] == f[14] == f[15] == f[16] == 0 and
                f[7:11] == (1, 0, 0, 0) and f[13] <= 4 and 0 < f[11] <= header[5],
                name + ': all actual generation/restart/wait/error/state/reserved fields')
        require((f[5], f[6]) == (actual_spawns[name], actual_spawns[name]) if f[5] > 0 else
                (f[5], f[6]) == (-1, -1), 'raw snapshot actual original PID/PGID binding')
        history, previous = [], 0
        for j in range(f[13]):
            t = struct.unpack_from('<Q4I', raw, offset + 160 + j * 24)
            require(0 < t[0] <= header[5] and t[0] >= previous and t[1] <= 8 and t[2] <= 8 and
                    1 <= t[3] <= 9 and t[4] == 0, 'chronological complete transition fields')
            previous = t[0]
            history.append(list(t))
        require(not any(raw[offset + 160 + f[13] * 24:offset + 256]), 'all unused history bytes zero')
        services[name], histories[name] = f, history
    require(set(services) == set(SERVICES) and not any(raw[832:]), 'all13 unused service entries zero')
    target = services['twohour-controller']
    require(target[1:4] == (6, 1, 3) and target[5:11] == (-1, -1, 1, 0, 0, 0) and
            target[12] == 0 and probe['supervisor_pid'] == actual_spawns['twohour-controller'],
            'original exited daemon NOT_READY/PIDPGID drained/raw exit0/no deadline')
    exits = [t for t in histories['twohour-controller'] if t[2:4] == [6, 5] and t[0] >= target[11]]
    require(exits, 'actual nonzero EXIT to EXITED transition')
    for owner, number in [('CONTROLLER', 1), ('WITNESS', sequence)]:
        line = f'WOS39_2H_{owner}_COMMITTED nonce={nonce:016x} sequence={number}\n'
        require(serial.count(line) == 1, 'actual single nonce-bound separate completion event')
    return {'qualification': 'PASS_SAVED_TWOHOUR_INIT_REAP_WIRE_ONLY', 'errors': [],
            'nonce': nonce, 'snapshot_sequence': sequence, 'published_mono_ns': header[5],
            'generation': 1, 'restart_count': 0, 'state': 6, 'readiness': 1,
            'raw_wait_status': 0, 'last_error': 0, 'exit_transitions': exits,
            'source_pins': pins, 'actual_service_spawns': actual_spawns,
            'service_fields': {n: list(f[1:]) for n, f in services.items()}, 'histories': histories,
            'runtime_qualified': False, 'criterion8_qualified': False, 'full_goal_qualified': False,
            'limits': ['Actual original init consuming reap only; witness later exit/natural poweroff unclaimed.']}


def validate_public_reports(files, serial):
    saved, pins = read_evidence(files)
    result = decode_saved_bytes(saved, serial)
    result['source_pins'] = pins
    return result


def validate_init_exit(files, probe, serial):
    saved, pins = read_evidence(files)
    return decode_init_saved_bytes(saved, probe, serial, pins)



def read_serial(path):
    path = Path(path)
    expected = path.lstat()
    require(stat.S_ISREG(expected.st_mode) and expected.st_nlink == 1 and
            0 < expected.st_size <= 64 * 2**20, 'bounded regular saved serial')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        require(identity(os.fstat(fd)) == identity(expected), 'saved serial exact open identity')
        copies = []
        for _ in range(2):
            os.lseek(fd, 0, os.SEEK_SET)
            data = bytearray()
            while len(data) <= expected.st_size:
                chunk = os.read(fd, min(65536, expected.st_size + 1 - len(data)))
                if not chunk:
                    break
                data.extend(chunk)
            require(len(data) == expected.st_size and
                    identity(os.fstat(fd)) == identity(expected), 'saved serial complete unchanged read')
            copies.append(bytes(data))
        require(copies[0] == copies[1] and identity(path.lstat()) == identity(expected),
                'saved serial double read and path identity')
    finally:
        os.close(fd)
    require(b'\0' not in copies[0], 'saved serial contains NUL')
    return copies[0].decode('utf-8', errors='strict')


def main():
    import argparse
    import json
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path, help='saved thirteen-file evidence directory')
    parser.add_argument('--serial', required=True, type=Path, help='saved whole serial stream')
    parser.add_argument('--host-receive', type=Path, help='optional saved independent live receiver ledger')
    args = parser.parse_args()
    try:
        files = {name: args.directory / name for name in EVIDENCE_NAMES}
        serial = read_serial(args.serial)
        guest = validate_public_reports(files, serial)
        init = validate_init_exit(files, guest, serial)
        receive = None
        if args.host_receive is not None:
            require(args.host_receive.stat().st_size <= 1 << 20, 'saved Host receive ledger exceeds1MiB')
            receipt = json.loads(read_serial(args.host_receive))
            receive = check_host_receive(receipt, guest['nonce'], serial.encode('utf-8'),
                                         init['snapshot_sequence'], guest['S_us'], guest['E_us'])
        value = {'qualification': 'PASS_SAVED_TWOHOUR_PROTOCOL_ONLY', 'errors': [],
                 'guest': guest, 'init': init, 'host_receive': receive, 'runtime_qualified': False,
                 'criterion8_qualified': False, 'full_goal_qualified': False,
                 'limits': ['External live Host receive timestamps and original process/VM identity are mandatory separate gates.',
                            'This decoder neither launches a VM nor proves artifact freshness, natural shutdown or image cleanup.']}
    except (OSError, ValueError, struct.error, OverflowError) as error:
        value = {'qualification': 'FAIL_SAVED_TWOHOUR_PROTOCOL',
                 'errors': [type(error).__name__ + ': ' + str(error)],
                 'runtime_qualified': False, 'criterion8_qualified': False, 'full_goal_qualified': False}
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0 if not value['errors'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
