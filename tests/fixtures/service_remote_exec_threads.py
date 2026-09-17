#!/usr/bin/env python3
"""Check old-thread retirement across exec of owned local and remote children."""
import argparse
import contextlib
import json
from pathlib import Path
import re
import shlex
import time

import paramiko
from service_ssh_broker_lifetime import connect
from service_wki_identity import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--peer', default='wos-3')
    parser.add_argument('--owner', default='wos-0')
    parser.add_argument('--binary', required=True)
    parser.add_argument('--retired-owner', action='store_true', help='join the retired main thread before worker exec')
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--fallback', action='store_true', help='exercise optional missing-target local fallback')
    modes.add_argument('--reject', action='store_true', help='exercise strict missing-target rejection and original-thread survival')
    parser.add_argument('--exit-status', type=int, choices=range(256), default=0,
                        metavar='0..255', help='replacement exit status to verify through the source parent')
    parser.add_argument('--output', type=Path, required=True)
    modes.add_argument('--receiver-reject-binary', help='payload linked to a missing interpreter for receiver rejection')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    helper = '/tmp/goal19-remote-exec-threads-' + str(time.time_ns())
    payload = helper + '-missing-interpreter' if args.receiver_reject_binary else helper
    (args.output / 'session.json').write_text(json.dumps({'helper': helper, 'payload': payload}, indent=2) + '\n')
    with contextlib.closing(paramiko.Agent()) as agent:
        with connect(args, agent.get_keys()[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.put(args.binary, helper)
            sftp.chmod(helper, 0o755)
            if args.receiver_reject_binary:
                sftp.put(args.receiver_reject_binary, payload)
                sftp.chmod(payload, 0o755)
            command_args = ['/usr/bin/locally', helper, args.peer, args.owner, f'/wki/{args.owner}' + payload,
                            '--exit-status', str(args.exit_status)]
            if args.fallback:
                command_args.append('--fallback')
            if args.reject:
                command_args.append('--reject')
            if args.retired_owner:
                command_args.append('--retired-owner')
            if args.receiver_reject_binary:
                command_args.append('--receiver-reject')
            command = shlex.join(command_args)
            status, output, error = run(transport, command)
            (args.output / 'stdout.txt').write_text(output + '\n')
            (args.output / 'stderr.txt').write_text(error)
            (args.output / 'result.json').write_text(json.dumps({'command': command, 'status': status}, indent=2) + '\n')
            print(output, flush=True)
            rows = [line for line in output.splitlines() if line.startswith(('PASS ', 'FAIL '))]
            if len(rows) != 8 or any(not row.endswith('reaped=1') for row in rows):
                raise RuntimeError('fixture needs child lifetime review before cleanup: ' + helper)
            if any('cleaned=1' not in row for row in rows):
                raise RuntimeError('fixture control directories retained: ' + helper)
            retirement = []
            for row in rows:
                if 'placement=remote' not in row:
                    continue
                match = re.search(r' receiver=(\d+) ', row)
                if not match or int(match[1]) <= 0:
                    raise RuntimeError('remote receiver identity unavailable: ' + helper)
                command = shlex.join(['/usr/bin/on', args.peer, '/usr/bin/test', '-d', '/proc/' + match[1]])
                deadline = time.monotonic() + 30
                while True:
                    retired, out, err = run(transport, command)
                    retirement.append({'command': command, 'status': retired, 'stdout': out, 'stderr': err})
                    (args.output / 'retirement.json').write_text(json.dumps(retirement, indent=2) + '\n')
                    if retired == 1 and not out and not err:
                        break
                    if retired != 0 or out or err or time.monotonic() >= deadline:
                        raise RuntimeError('remote receiver lifetime requires review: ' + helper)
                    time.sleep(0.1)
            sftp.remove(helper)
            if args.receiver_reject_binary:
                sftp.remove(payload)
            print('fixture cleanup completed', flush=True)
            if status or error or any(row.startswith('FAIL ') for row in rows):
                raise SystemExit(1)


if __name__ == '__main__':
    main()
