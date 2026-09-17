#!/usr/bin/env python3
"""Exercise native mmap authorization on TMPFS/XFS with root and UID100."""
import argparse
import contextlib
import json
from pathlib import Path
import re
import shlex
import socket
import time

import paramiko

from service_ssh_broker_lifetime import connect


def run(transport, command):
    with transport.open_session(timeout=10) as channel:
        channel.settimeout(5)
        channel.exec_command(command)
        output = bytearray()
        last_notice = time.monotonic()
        while True:
            try:
                chunk = channel.recv(4096)
            except socket.timeout:
                if time.monotonic() - last_notice >= 30:
                    print('waiting on the same fixture channel', flush=True)
                    last_notice = time.monotonic()
                continue
            if not chunk:
                break
            output.extend(chunk)
            if len(output) > 65536:
                raise RuntimeError('unexpected fixture output size')
        return channel.recv_exit_status(), output.decode(), channel.recv_stderr(4096).decode()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(exist_ok=False, parents=True)
    suffix = str(time.time_ns())
    image = '/tmp/goal19-mmap-policy-' + suffix
    results = []
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError('authorized SSH agent key required')
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.get_channel().settimeout(30)
            sftp.put(args.binary, image)
            try:
                sftp.chmod(image, 0o755)
                for backend, root in [('tmpfs', '/tmp'), ('xfs', '')]:
                    base = root + '/goal19-mmap-files-' + suffix
                    status, stdout, stderr = run(transport, shlex.join(['/usr/bin/locally', image, base]))
                    (args.output / (backend + '.txt')).write_text(stdout)
                    cases = re.findall(r'^(PASS|FAIL) profile=(\d+) case=(\d+) mode=(\d+) shared=(\d+) prot=(\d+) mmap_errno=(\d+) mprotect_errno=(\d+) cleanup=(\d+)$', stdout, re.M)
                    passed = status == 0 and not stderr and len(cases) == 36 and all(row[0] == 'PASS' and row[-1] == '1' for row in cases) and \
                        'PASS mmap policy checks=36 failed_profiles=0 cleanup=1' in stdout
                    results.append({'backend': backend, 'status': status, 'stderr': stderr, 'cases': cases, 'pass': passed, 'base': base})
                    (args.output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
                    print(backend + '\n' + stdout, flush=True)
            finally:
                sftp.remove(image)
                print('uploaded fixture removed', flush=True)
    if len(results) != 2 or not all(row['pass'] for row in results):
        raise RuntimeError('mmap policy failed; inspect per-backend evidence')


if __name__ == '__main__':
    main()
