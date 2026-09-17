#!/usr/bin/env python3
"""Run local IPv6 authority authority/restore checks on each disposable WOS cluster node."""
import argparse
import contextlib
import errno
import json
import re
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
    base = '/tmp/goal19-ipv6-authority-' + str(time.time_ns())
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
                    text = result.decode()
                    pids = [int(value) for value in re.findall(r'(?:parent|pid)=(\d+)', text)]
                    probes = []
                    for pid in pids:
                        probe = f'/proc/{pid}' if local else f'/wki/{node}/proc/{pid}'
                        absent = run(transport, shlex.join(['/usr/bin/locally', '/usr/bin/test', '!', '-e', probe]))
                        probes.append({'node': node, 'pid': pid, 'result': absent})
                        if absent[0] != 0:
                            raise RuntimeError(f'fixture task remains: {node}/{pid}')
                    (args.output / (node + '-probes.json')).write_text(json.dumps(probes, indent=2) + '\n')
                    if status or err or b'PASS: IPv6 authority profiles=6 checks=78 failures=0' not in result or result.count(b' ok=1') != 78:
                        raise RuntimeError(f'{node}: IPv6 authority functional checks failed')
            finally:
                for path in reversed(cleanup):
                    try:
                        sftp.remove(path)
                    except OSError as error:
                        if error.errno != errno.ENOENT:
                            raise
                for path in cleanup:
                    try:
                        sftp.stat(path)
                    except OSError as error:
                        if error.errno == errno.ENOENT:
                            continue
                        raise
                    raise RuntimeError(f'helper/report remains: {path}')
                (args.output / 'cleanup.json').write_text(json.dumps({'absent': cleanup}, indent=2) + '\n')
                print('fixture files removed', flush=True)
    print('PASS: 156 IPv6 authority authorization checks across both nodes; loopback records restored', flush=True)


if __name__ == '__main__':
    main()
