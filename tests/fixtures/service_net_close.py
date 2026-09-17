#!/usr/bin/env python3
"""Check mapped remote NIC CLOSE/reopen through NETCTL and legacy IOCTL on disposable nodes."""
import argparse
import contextlib
import errno
from pathlib import Path
import shlex
import time

import paramiko
from service_ssh_broker_lifetime import connect
from service_wki_identity import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--binary', required=True)
    parser.add_argument('--peer', default='wos-3')
    parser.add_argument('--owner-error', type=int, default=0)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    base = '/tmp/goal19-net-close-' + str(time.time_ns())
    helper, before, after, result = [base + suffix for suffix in ('-helper', '-before', '-after', '-result')]
    remote_result = '/wki/' + args.peer + result
    with contextlib.closing(paramiko.Agent()) as agent:
        with connect(args, agent.get_keys()[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            cleanup = []
            try:
                sftp.put(args.binary, helper)
                sftp.chmod(helper, 0o755)
                cleanup.append(helper)
                commands = [
                    (['/usr/bin/locally', helper, '--snapshot', before, 'wos-0'], before),
                    (['/usr/bin/on', args.peer, '/wki/wos-0' + helper, str(args.owner_error), result, args.peer], remote_result),
                    (['/usr/bin/locally', helper, '--snapshot', after, 'wos-0'], after),
                ]
                for i, (argv, path) in enumerate(commands):
                    status, stdout, stderr = run(transport, shlex.join(argv))
                    (args.output / f'command-{i}.txt').write_text(f'exit={status}\nstdout={stdout}\nstderr={stderr}\n')
                    cleanup.append(path)
                    try:
                        with sftp.open(path, 'rb') as report:
                            (args.output / f'report-{i}.txt').write_bytes(report.read())
                    except OSError as error:
                        if error.errno != errno.ENOENT:
                            raise
                    if status or stderr:
                        raise RuntimeError(f'command {i} failed: exit={status}, stderr={stderr!r}')
                def read(path):
                    with sftp.open(path, 'rb') as file:
                        return file.read()
                result_bytes = read(remote_result)
                args.output.joinpath('profiles.txt').write_bytes(result_bytes)
                print(result_bytes.decode(), flush=True)
                if b'PASS: remote close profiles=6 failures=0' not in result_bytes or result_bytes.count(b'preserved=1 restored=1') != 12:
                    raise RuntimeError('profile report incomplete or failed')
                owner_before, owner_after = read(before), read(after)
                args.output.joinpath('owner-before.txt').write_bytes(owner_before)
                args.output.joinpath('owner-after.txt').write_bytes(owner_after)
                if not owner_before or owner_before != owner_after:
                    raise RuntimeError('owner interface records changed')
            finally:
                for path in reversed(cleanup):
                    try:
                        sftp.remove(path)
                    except OSError as error:
                        if error.errno != errno.ENOENT:
                            raise
                print('fixture files removed', flush=True)
    print('PASS: mapped NIC CLOSE/reopen and owner interface preservation', flush=True)


if __name__ == '__main__':
    main()
