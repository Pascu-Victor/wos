#!/usr/bin/env python3
"""Strict saved-report decoder; actual init spawn/raw wait remains a separate gate."""
import argparse
import json
from pathlib import Path
import re
import struct

FIELDS = (
    'version bytes nonce case_id actor_pid root_serial checks valid operation_result expected_result '
    'source_ref1 resident_ref1 selected_pages used_before used_swapped used_after_copy total_before total_active '
    'total_after_cleanup free_active used_after_cleanup block_writes block_reads history_overflow '
    'pattern_bytes_checked guard_bytes_checked mapping_consumed cleanup_complete task_access_after '
    'root_readers_after futex_waiter_after reserved'
).split()
FORMAT = struct.Struct('<8Q2q22Q')
assert FORMAT.size == 256 and len(FIELDS) == 32


def decode(reports, witness, nonce, pid):
    if not 0 < nonce < 2**64 or not 1 < pid < 2**64 or len(reports) != 1024 or len(witness) > 256:
        raise ValueError('invalid identity or exact transport length')
    expected_witness = f'anonymous_usercopy_v1 nonce={nonce} pid={pid} ppid=1 cases=4 reports=1024 status=0\n'.encode('ascii')
    if witness != expected_witness:
        raise ValueError('witness does not join exact nonce/PID/parent/status')
    rows = []
    root = None
    for index in range(4):
        row = dict(zip(FIELDS, FORMAT.unpack_from(reports, index * 256)))
        expected = dict.fromkeys(FIELDS, 0)
        expected.update(version=1, bytes=256, nonce=nonce, case_id=index, actor_pid=pid, root_serial=row['root_serial'],
                        checks=2, valid=1, operation_result=128 if index == 1 else -110 if index == 3 else 4096,
                        expected_result=128 if index == 1 else -110 if index == 3 else 4096,
                        source_ref1=1, resident_ref1=1, selected_pages=1, used_swapped=1, total_active=16, free_active=16,
                        block_writes=1, block_reads=1, pattern_bytes_checked=8192, guard_bytes_checked=4096,
                        mapping_consumed=1, cleanup_complete=1)
        if row != expected or row['root_serial'] == 0 or (root is not None and row['root_serial'] != root):
            raise ValueError(f'case {index}: result/data/ref/slot/history/cleanup mismatch')
        root = row['root_serial']
        rows.append(row)
    return dict(result='PASS_SAVED_REPORTS_ONLY', runtime_qualified=False, actual_init_join_required=True,
                nonce=nonce, pid=pid, root_serial=root, rows=rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reports', type=Path, required=True)
    parser.add_argument('--witness', type=Path, required=True)
    parser.add_argument('--nonce', type=int, required=True)
    parser.add_argument('--pid', type=int, required=True)
    args = parser.parse_args()
    if args.reports.stat().st_size != 1024 or args.witness.stat().st_size > 256:
        parser.error('oversize or incomplete saved transport')
    try:
        result = decode(args.reports.read_bytes(), args.witness.read_bytes(), args.nonce, args.pid)
    except (ValueError, OSError) as error:
        parser.exit(1, f'FAIL_SAVED_REPORTS: {error}\n')
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
