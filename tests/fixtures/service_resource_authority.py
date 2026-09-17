#!/usr/bin/env python3
"""Run service and process resource authority checks on each disposable WOS cluster node."""
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
    parser.add_argument('--worker', action='store_true', help='Run each child profile from a pthread')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    base = '/tmp/goal19-resource-' + str(time.time_ns())
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
                    if args.worker:
                        argv.append('--worker')
                    status, out, err = run(transport, shlex.join(argv))
                    (args.output / (node + '-command.txt')).write_text(f'exit={status}\nstdout={out}\nstderr={err}\n')
                    with sftp.open(path, 'rb') as file:
                        result = file.read()
                    (args.output / (node + '-report.txt')).write_bytes(result)
                    print(result.decode(), flush=True)
                    execution = b'execution=worker' if args.worker else b'execution=main'
                    if (status or err or execution not in result or
                            b'PASS: resources profiles=6 checks=54 failures=0' not in result or result.count(b'preserved=1') != 54):
                        raise RuntimeError(f'{node}: resource functional checks failed')
            finally:
                for path in reversed(cleanup):
                    try:
                        sftp.remove(path)
                    except OSError as error:
                        if error.errno != errno.ENOENT:
                            raise
                print('fixture files removed', flush=True)
    print('PASS: 108 resource authorization checks across both nodes; child limits isolated', flush=True)


if __name__ == '__main__':
    main()
