#!/usr/bin/env python3
"""Strict bounded user-wire decoder. It never qualifies a kernel census or VM."""
import argparse
import hashlib
import json
from pathlib import Path
import stat
import struct

MASK = (1 << 64) - 1
SEAL = 0xa69e572d43f18bc5
MANIFEST_MAGIC = 0x43394d414e494645
HEADER_MAGIC = 0x4339484541444552
ATTEMPT_MAGIC = 0x4339415454454d50
STATUS_MAGIC = 0x4339535441545553
FD_MAGIC = 0x433946444c454447
ATTEMPTS = 1032
SIZES = {'control.bin': 4096, 'child.raw': 8192, 'attempts.bin': 528384,
         'evidence.bin': 4227072, 'fd-ledger.bin': 8192, 'driver-status': 512,
         'wos-lifecycle60-manifest.bin': 1024}
ERRORS = (-12, -12, -12, -12, -12, -125, -12, -12, -12, -12, -125, -125,
          -12, -12, -12, -12, -5, -12, -125, -125, -11, -11, -12, -12)


def require(ok, message):
    if not ok:
        raise ValueError(message)


def words(data, count):
    require(type(data) is bytes and len(data) == count * 8, 'exact LE64 extent')
    return struct.unpack('<' + str(count) + 'Q', data)


def signed(value):
    return value if value < 1 << 63 else value - (1 << 64)


def hash_words(row):
    value = 1469598103934665603
    for word in row:
        for byte in range(8):
            value = ((value ^ ((word >> (8 * byte)) & 255)) * 1099511628211) & MASK
    return value


def sealed(row, magic, declared_bytes=512):
    require(len(row) == 64 and row[:3] == (magic, 1, declared_bytes), 'exact record role/version/bytes')
    require(row[62] == hash_words(row[:62]) and row[63] == ((row[62] ^ SEAL) | 1),
            'complete integrity checksum/commit; not authentication')


def user_pointer(value, length=1):
    return 4096 <= value < 0x800000000000 and length <= 0x800000000000 - value


def manifest(data):
    raw = words(data, 128)
    require(raw[:4] == (MANIFEST_MAGIC, 1, 1024, 4) and raw[4] > 0 and raw[5:8] == (8, 24, 1000),
            'fixed four-actual-image manifest and exact campaign')
    require(raw[72:] == (0,) * 56, 'manifest tail reserved zero')
    entries = []
    for i in range(4):
        offset = 64 + 128 * i
        role, size, elf_type, entry, phnum = struct.unpack_from('<5Q', data, offset)
        digest = data[offset + 40:offset + 72]
        require(role == i + 1 and 0 < size <= (4 << 20 if i == 0 else 65536) and
                elf_type == (2 if i in (1, 2) else 3) and entry > 0 and 0 < phnum <= 64 and
                any(digest) and not any(data[offset + 72:offset + 128]), 'bounded actual-image entry/zero reserved')
        entries.append({'role': role, 'bytes': size, 'type': elf_type, 'entry': entry,
                        'phnum': phnum, 'sha256': digest.hex()})
    return raw[4], entries


def check_input(h, row, nonce, parent, limits):
    index, mode = row[4], row[7]
    require(h[:7] == (0x4131524157494e50, 1, 512, nonce, index + 1, mode, parent), 'actual per-attempt child input identity')
    require(h[7] == row[9] and h[7] <= MASK - 60000000 and h[8] == h[7] + 60000000 == row[10] and
            h[9:16] == (100, 1, 0, 256, 0, 0, 0), 'sixty-second input deadline and exact locked private credentials')
    require(h[16:48] == limits and h[16:18] == (10, 10) and
            all(h[16 + 2*i] <= h[17 + 2*i] for i in range(16)) and
            8192 <= h[18] <= 1048576 and 8192 <= h[19] <= 1048576 and
            10 <= h[30] <= 256 and 10 <= h[31] <= 256, 'all16 fixed policy pairs and bounded actual child ceilings')
    require(h[48:56] == (4096, 2048, 8192, 32, 0x4141, 4096, 1, 0) and
            h[56:58] == (row[28], row[27]) and signed(h[58]) >= 0 and h[58] & 3 == 2 and
            h[59:64] == (1024, 256, 16, 10000, nonce), 'actual grant File identity/rights/input commit')
    require(hash_words(h) == row[23], 'input content bound by final attempt row')


def check_ready(h, r, row, entries):
    mode, nonce, ident = h[5], h[3], h[4]
    main = entries[mode]
    interp = entries[3]
    require(r[:10] == (0x4131524157525054, 1, 2048, 1, nonce, ident, mode, mode, 1, 0) and
            r[10:21] == (100, 100, 100, 100, 1, 0, 256, 255 if mode == 2 else 127, 0, 0, 0) and r[21] > 0,
            'real initial entry, localPID1/PPID0, credentials/NNP/authority/service, no raw failure')
    require(r[22:24] == (h[6], h[7]) and h[7] <= r[24] <= r[25] <= row[13] < h[8] and
            r[26:32] == (h[8], 16, 255, h[58], h[56], h[57]) and r[32:48] == (0,) * 16 and r[64:96] == h[16:48],
            'complete actual sixteen-round READY/255 absent descriptors/all16 GETRLIMIT results')
    require(r[48] < 256 and r[48] != 9 and signed(r[49]) >= 0 and r[49] & 3 == 2 and r[50] == 0 and
            0 < r[51] <= 0x7fffffff and user_pointer(r[52], 4096) and r[52] % 4096 == 0 and
            r[53:59] == (r[51], 0, MASK, MASK, 16, MASK ^ 16), 'actual File/SysV work, same-key lookup and IPv4/IPv6 denial')
    require(r[61:64] == (32, 0o100600, 100) and user_pointer(r[96], 1024) and r[96] % 16 == 0 and
            r[97:99] == (main['entry'],) * 2 and r[105:112] == (1, 0, 0x1040, 56, main['phnum'], 4096, 0),
            'real stack/main entry/copied PHDR/auxv shape and private work File')
    if mode == 2:
        require(0x30000000 <= r[99] < 0x70000000 and r[99] % 0x200000 == 0 and
                r[100] == r[99] + interp['entry'] <= MASK and r[101] == 0x54494e593431484f,
                'actual tiny-interpreter bias/entry/handoff')
    else:
        require(r[99:101] == (0, 0), 'static entry has no interpreter BASE/start')
    # Static incoming R12 and incoming RDI/RSI/RDX are deliberately opaque.
    require(r[112:128] == (8192, 0, 512, 100, 0x4141, 4096, 100, 100, 0o600, 1,
                           nonce, ident, 16, MASK ^ 16, nonce, ident), 'actual input/File/SysV identity and payload')
    for offset, value in ((128, 32), (144, 32), (160, r[51]), (176, 0), (192, 1), (208, 1)):
        require(r[offset:offset + 16] == (value,) * 16, 'every actual initial work result ' + str(offset))
    require(r[224:234] == (nonce, 0, 1, 100, 100, 100, 100, 1, 1, 1 if mode == 2 else 0) and
            user_pointer(r[234], 16) and user_pointer(r[235], 19) and
            r[236:240] == (13 if mode == 2 else 12, interp['entry'] if mode == 2 else 0, 0, 0) and
            r[240:255] == (0,) * 15 and r[255] == nonce and hash_words(r) == row[24], 'full startup/aux/clock/READY commit')


def check_heartbeat(h, r, b, expected_round, observed, expected_hash):
    require(b[:5] == (0x41314c4956454842, 1, h[3], h[4], h[5]) and
            b[5] == expected_round and 17 <= b[5] <= 10000 and r[25] <= b[6] <= observed < h[8] and
            b[7:13] == (32, 32, r[51], 0, b[5], MASK ^ b[5]) and b[13] >= r[21] and
            b[14:24] == (0, 0, r[48], r[51], r[52], 0, 1, 1, 100, 1) and
            b[24:31] == (0,) * 7 and b[31] == b[5] and hash_words(b) == expected_hash,
            'complete actual continuing File/SysV work and heartbeat commit')


def _decode(saved, partial=False):
    require(type(saved) is dict and set(saved) == set(SIZES), 'exact seven complete saved protocol buffers')
    require(all(type(saved[n]) is bytes and len(saved[n]) == s for n, s in SIZES.items()), 'every protocol extent exact before decoding')
    nonce, entries = manifest(saved['wos-lifecycle60-manifest.bin'])
    control = saved['control.bin']
    header = words(control[:512], 64)
    sealed(header, HEADER_MAGIC)
    require(header[3] == nonce and header[4] > 2 and header[5] == 1 and
            header[6:16] == (1032, 8, 24, 1000, 512, 4096, 8192, 4096, 8192, 512) and
            0 < header[16] <= MASK - 3600000000 and header[17] == header[16] + 3600000000,
            'fixed campaign, actual original Host-parent identity and absolute deadline')
    require(header[18:24] == (0,) * 6 and header[33:62] == (0,) * 29 and not any(control[1024:]), 'all control reserved bytes zero')
    fds = header[24:33]
    require(all(fd < 256 for fd in fds) and len(set(fds)) == 9, 'nine actual distinct persistent descriptors')
    ledger = words(saved['fd-ledger.bin'], 1024)
    sealed(ledger[:64], FD_MAGIC, 8192)
    require(ledger[:4] == (FD_MAGIC, 1, 8192, nonce), 'fixed FD ledger header extent')
    require(ledger[4] == header[4] and ledger[6:8] == (256, 2) and ledger[8:62] == (0,) * 54 and ledger[576:] == (0,) * 448,
            'exact FD identity/array count/reserved storage')
    installed = 0
    for fd in range(256):
        getfd, getfl = signed(ledger[64 + fd]), signed(ledger[320 + fd])
        if getfd == -9:
            require(getfl == -9, 'absent FD has no status flags')
        else:
            require(getfd >= 0 and getfl >= 0 and fd in fds,
                    'every installed actual descriptor is one of the nine private ledger Files')
            installed += 1
    require(installed == ledger[5] == 9, 'full256 FD census contains exactly nine private descriptors')
    for role, fd in enumerate(fds):
        require(ledger[64 + fd] == 1 and ledger[320 + fd] & 3 == (0 if role < 3 else 2) and
                ledger[320 + fd] & 0o100000000, 'persistent source/IO rights, exact CLOEXEC and NO_CACHE')
    ledger_hash = hash_words(ledger)
    status = words(saved['driver-status'], 64)
    if not partial:
        sealed(status, STATUS_MAGIC)
        require(status[3:9] == (nonce, 1032, 8, 24, 1000, 1032) and
                status[9:15] == (header[16], header[17], header[4], 1, 0, 0) and
                status[15] > 0 and header[16] < status[16] < header[17] and status[17] == ledger_hash and
                status[18:62] == (0,) * 44, 'strict completed user summary; no kernel PASS field')
    child_pids = set()
    previous_end, previous_checks = header[16], 0
    limits = None
    source_identities = {}
    interpreter_identity = None
    final_row = None
    prefix_stop = None
    for index in range(ATTEMPTS):
        row = words(saved['attempts.bin'][index * 512:(index + 1) * 512], 64)
        if partial and (not any(row) or row[8] != 3 or row[31] != 0):
            require(not any(saved['attempts.bin'][(index + 1) * 512:]) and
                    not any(saved['evidence.bin'][(index + 1) * 4096:]),
                    'strict prefix has no later attempted record or evidence')
            prefix_stop = {'index': index, 'committed': bool(any(row)), 'protocol_credit': False}
            if any(row):
                sealed(row, ATTEMPT_MAGIC)
                kind = 0 if index < 8 else 1 if index < 32 else 2
                variant = 2 if kind == 1 else 1 + (index & 1)
                require(row[3:8] == (nonce, index, index + 1, kind, variant) and row[8] in (1, 2, 3) and
                        row[32] == ledger_hash and row[39] == header[4] and row[44:62] == (0,) * 18,
                        'next prefix row exact identity/private ledger/reserved fields')
                require(previous_end <= row[9] < row[10] < header[17], 'prefix bounded input interval')
                evidence = saved['evidence.bin'][index * 4096:(index + 1) * 4096]
                h = words(evidence[:512], 64)
                check_input(h, row, nonce, header[4], h[16:48] if limits is None else limits)
                require(row[29] == fds[3] and h[58] == ledger[320 + fds[3]] and not any(evidence[3072:]),
                        'prefix actual grant rights and zero evidence tail')
                if row[8] == 1:
                    require(row[11:23] == (1 << 63, 0, 0, 0, 0, 0, 0, 0, 1 << 63, 1 << 63, 0, 0) and
                            row[24:27] == (0, 0, 0) and row[30:32] == (0, 0) and
                            row[40:44] == (0, 0, 0, 0) and
                            not any(evidence[512:]), 'REQUEST contains no future user results/work')
                elif row[8] == 2:
                    require(kind != 1 and 2 < row[12] < 1 << 63 and row[12] != header[4] and row[11] == row[12] and
                            row[12] not in child_pids and row[19:23] == (0, 1 << 63, 0, 0) and row[31] == 0 and
                            row[40] > 0 and row[41:44] == (0, 0, 0), 'BEFORE_WAIT has no future consuming result')
                    ready = words(evidence[512:2560], 256)
                    a, b = words(evidence[2560:2816], 32), words(evidence[2816:3072], 32)
                    check_ready(h, ready, row, entries)
                    check_heartbeat(h, ready, a, row[14], row[16], row[25])
                    check_heartbeat(h, ready, b, row[15], row[17], row[26])
                    require(b[5] > a[5] and b[13] > a[13] and b[6] >= a[6] and
                            row[9] <= row[30] <= row[13] <= row[16] <= row[17] <= row[18] < row[10],
                            'BEFORE_WAIT real advancing work precedes successful kill')
                else:
                    require(row[31] > 0, 'COMPLETED prefix stop is an explicit failure')
                prefix_stop.update(state=row[8], failure_stage=row[31])
            break
        sealed(row, ATTEMPT_MAGIC)
        kind = 0 if index < 8 else 1 if index < 32 else 2
        variant = 2 if kind == 1 else 1 + (index & 1)
        require(row[3:9] == (nonce, index, index + 1, kind, variant, 3) and row[31] == 0 and
                row[32] == ledger_hash and row[39] == header[4] and row[40] > previous_checks and
                row[44:62] == (0,) * 18, 'exact contiguous completed attempt/index/variant/no failure/reserved')
        require(previous_end <= row[9] <= row[30] <= row[41] < row[10] < header[17], 'ordered finite actual per-attempt timestamps')
        require(row[27:30] == (words(saved['attempts.bin'][:512], 64)[27], words(saved['attempts.bin'][:512], 64)[28], fds[3]),
                'same retained report File/dev/ino/FD throughout')
        require(row[35] == entries[variant]['bytes'] and row[34] > 0 and
                (variant == 2 or row[36:39] == (0, 0, 0)) and
                (variant != 2 or row[37] > 0 and row[38] == entries[3]['bytes']), 'selected actual asset metadata')
        require(source_identities.setdefault(variant, row[33:36]) == row[33:36],
                'same retained main asset identity for each variant')
        if variant == 2:
            if interpreter_identity is None:
                interpreter_identity = row[36:39]
            require(interpreter_identity == row[36:39], 'same retained interpreter asset identity')
        evidence = saved['evidence.bin'][index * 4096:(index + 1) * 4096]
        h, ready = words(evidence[:512], 64), words(evidence[512:2560], 256)
        a, b = words(evidence[2560:2816], 32), words(evidence[2816:3072], 32)
        require(not any(evidence[3072:]), 'per-attempt evidence tail zero')
        if limits is None:
            limits = h[16:48]
        check_input(h, row, nonce, header[4], limits)
        require(h[58] == ledger[320 + fds[3]], 'child actual shared report rights match persistent FD ledger')
        if kind == 1:
            require(signed(row[11]) == ERRORS[index - 8] and row[12:27] == (0,) * 7 + (1 << 63, 1 << 63) + (0, 0, row[23], 0, 0, 0),
                    'fault user return matches fixed errno with no invented child/kill/wait/work')
            require(row[42:44] == (0, 0), 'failed CREATE has no invented wait attempts')
            require(not any(evidence[512:]), 'fault evidence never invents actual user entry/work')
        else:
            pid = signed(row[11])
            require(2 < pid < 1 << 63 and pid == row[12] == signed(row[20]) and pid != header[4] and pid not in child_pids and
                    row[19] == 0 and row[21] == 9 and row[22] > 0, 'distinct actual positive CREATE, successful SIGKILL and consuming wait/raw9')
            child_pids.add(pid)
            check_ready(h, ready, row, entries)
            check_heartbeat(h, ready, a, row[14], row[16], row[25])
            check_heartbeat(h, ready, b, row[15], row[17], row[26])
            require(b[5] > a[5] and b[13] > a[13] and b[6] >= a[6] and
                    row[30] <= row[13] <= row[16] <= row[17] <= row[18] <= row[22] <= row[41],
                    'two advancing real work rounds before actual kill/consuming wait and terminal record')
        previous_end, previous_checks = row[41], row[40]
        final_row = row
    if partial:
        completed = ATTEMPTS if prefix_stop is None else prefix_stop['index']
        current = words(control[512:1024], 64)
        allowed = [(0,) * 64] if final_row is None else [final_row]
        if prefix_stop is not None and prefix_stop['committed']:
            offset = prefix_stop['index'] * 512
            allowed.append(words(saved['attempts.bin'][offset:offset + 512], 64))
        require(current in allowed, 'prefix control is exactly a completed or next committed ledger row')
        return {'qualification': 'SAVED_USER_LIFECYCLE60_PREFIX_DIAGNOSTIC_ONLY', 'errors': [],
                'nonce': nonce, 'driver_pid': header[4], 'validated_completed_prefix': completed,
                'positive_child_wait9_prefix': len(child_pids), 'next_record': prefix_stop,
                'terminal_protocol_qualified': False, 'fault_consumption_qualified': False,
                'Task_deletion_qualified': False, 'executor_RETURNED_qualified': False,
                'versioned_census_qualified': False, 'driver_init_reap_qualified': False,
                'VM_runtime_qualified': False, 'criterion9_qualified': False, 'full_goal_complete': False}
    require(len(child_pids) == 1008 and status[15] >= previous_checks and status[16] >= previous_end,
            'eight warmups plus1000 user-positive rows and terminal checks/time cover last completed record')
    current = words(control[512:1024], 64)
    require(current == final_row, 'control current record exactly last completed attempt; no extra CREATE')
    last = saved['evidence.bin'][-4096:]
    raw = saved['child.raw']
    require(raw[:512] == last[:512] and raw[4096:6144] == last[512:2560] and
            not any(raw[512:1024]) and not any(raw[1280:4096]) and not any(raw[6144:]), 'last immutable input/READY and unused raw regions held')
    return {'qualification': 'PASS_SAVED_USER_LIFECYCLE60_PROTOCOL_ONLY', 'errors': [], 'nonce': nonce,
            'driver_pid': header[4], 'attempts': 1032, 'warmups': 8, 'fault_errno_rows': 24,
            'positive_child_wait9_rows': 1008, 'counted_success_rows': 1000,
            'manifest_images': entries, 'fixed_reports_bytes': sum(SIZES.values()),
            'source_pins': {n: {'bytes': len(saved[n]), 'sha256': hashlib.sha256(saved[n]).hexdigest()} for n in SIZES},
            'fault_consumption_qualified': False, 'Task_deletion_qualified': False,
            'executor_RETURNED_qualified': False, 'versioned_census_qualified': False,
            'driver_init_reap_qualified': False, 'VM_runtime_qualified': False,
            'criterion9_qualified': False, 'full_goal_complete': False}


def decode(saved):
    return _decode(saved, partial=False)


def scan_prefix(saved):
    return _decode(saved, partial=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--prefix', action='store_true', help='diagnose a strict saved prefix without terminal or kernel credit')
    args = parser.parse_args()
    saved = {}
    for name, size in SIZES.items():
        path = args.directory / name
        require(not path.is_symlink(), 'no saved report symlink')
        before = path.stat()
        require(stat.S_ISREG(before.st_mode) and before.st_size == size, 'exact bounded saved regular report')
        saved[name] = path.read_bytes()
        after = path.stat()
        require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns), 'saved report stable through read')
    print(json.dumps(scan_prefix(saved) if args.prefix else decode(saved), indent=2))


if __name__ == '__main__':
    main()
