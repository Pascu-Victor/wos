#!/usr/bin/env python3
"""Check cwd client mount policy across ordinary local and remote exec."""
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


def run_stream(transport, command, destination):
    # An idle observation is not evidence that the guest command stopped.
    # Keep the same channel and preserve partial output until real EOF/status.
    with transport.open_session(timeout=10) as channel, destination.open('w') as log:
        channel.settimeout(5)
        channel.exec_command(command)
        output = bytearray()
        last_report = time.monotonic()
        while True:
            try:
                chunk = channel.recv(4096)
            except socket.timeout:
                if time.monotonic() - last_report >= 30:
                    print('still observing the same guest command: ' + str(destination), flush=True)
                    last_report = time.monotonic()
                continue
            if not chunk:
                break
            output.extend(chunk)
            text = chunk.decode(errors='replace')
            log.write(text)
            log.flush()
            print(text, end='', flush=True)
            if len(output) > 1024 * 1024:
                raise RuntimeError('fixture exceeded output bound; partial output preserved')
        error = channel.recv_stderr(4096).decode()
        return channel.recv_exit_status(), output.decode().strip(), error.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--peer', default='wos-3')
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    suffix = str(time.time_ns())
    image = '/tmp/goal19-cwd-policy-' + suffix
    owner_image = '/wki/' + args.peer + image
    results = []
    (args.output / 'invocation.json').write_text(json.dumps({'suffix': suffix, 'image': image, 'owner_image': owner_image}, indent=2) + '\n')
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError('authorized SSH agent key required')
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.get_channel().settimeout(30)
            uploaded = []
            try:
                # /tmp is receiver-local under the normal placement policy.
                # Provision both images; the fixture uses a canonical peer mount
                # so cwd localization reaches the same owner inode.
                for destination in (image, owner_image):
                    sftp.put(args.binary, destination)
                    uploaded.append(destination)
                    sftp.chmod(destination, 0o755)
                for backend, export in [('tmpfs', '/tmp'), ('xfs', '/')]:
                    status, stdout, stderr = run_stream(transport, shlex.join(['/usr/bin/locally', image, args.peer, export, suffix + '-' + backend]), args.output / (backend + '.stream.txt'))
                    (args.output / (backend + '.txt')).write_text(stdout + '\n')
                    cases = re.findall(r'^(PASS|FAIL) case readonly=(\d) profile=(\d+) worker=(\d) remote=(\d) status=(\d+) owner_data=(\d)$', stdout, re.M)
                    passed = status == 0 and not stderr and len(cases) == 16 and all(row[0] == 'PASS' for row in cases) and \
                        'PASS cwd mount policy checks=16 failed=0 cleanup=1' in stdout
                    results.append({'backend': backend, 'status': status, 'stderr': stderr, 'cases': cases, 'pass': passed})
                    (args.output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
                    print(backend + '\n' + stdout, flush=True)
            finally:
                for destination in reversed(uploaded):
                    sftp.remove(destination)
                print('uploaded images removed', flush=True)
    if len(results) != 2 or not all(row['pass'] for row in results):
        raise RuntimeError('cwd mount policy failed; see per-backend results')


if __name__ == '__main__':
    main()
