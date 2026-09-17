#!/usr/bin/env python3
"""Run ptrace image-list access and dumpability checks on each disposable WOS cluster node."""
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
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    base = '/tmp/goal19-ptrace-images-' + str(time.time_ns())
    helper = base + '-helper'
    with contextlib.closing(paramiko.Agent()) as agent:
        with connect(args, agent.get_keys()[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            cleanup = []
            try:
                cleanup.append(helper)
                sftp.put(args.binary, helper)
                sftp.chmod(helper, 0o755)
                for node in ('wos-0', 'wos-3'):
                    report = base + '-' + node
                    local = node == 'wos-0'
                    path = report if local else '/wki/' + node + report
                    cleanup.append(path)
                    argv = ['/usr/bin/locally', helper, report, node] if local else [
                        '/usr/bin/on', node, '/wki/wos-0' + helper, report, node]
                    status, out, err = run(transport, shlex.join(argv))
                    (args.output / (node + '-command.txt')).write_text(f'exit={status}\nstdout={out}\nstderr={err}\n')
                    with sftp.open(path, 'rb') as file:
                        result = file.read()
                    (args.output / (node + '-report.txt')).write_bytes(result)
                    print(result.decode(), flush=True)
                    if status or err or b'PASS: ptrace-images checks=14 failures=0' not in result or result.count(b'preserved=1') != 14:
                        raise RuntimeError(f'{node}: ptrace image-list functional checks failed')
            finally:
                for path in reversed(cleanup):
                    try:
                        sftp.remove(path)
                    except OSError as error:
                        if error.errno != errno.ENOENT:
                            raise
                print('fixture files removed', flush=True)
    print('PASS: 28 ptrace image-list checks across both nodes; targets reaped', flush=True)


if __name__ == '__main__':
    main()
