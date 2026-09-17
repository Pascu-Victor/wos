#!/usr/bin/env python3
"""Compare signal state across direct local and remote exec of owned children."""
import argparse
import contextlib
import json
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
    parser.add_argument('--peer', default='wos-3')
    parser.add_argument('--owner', default='wos-0')
    parser.add_argument('--binary', required=True)
    parser.add_argument('--handler', action='store_true', help='exec from an alternate-stack signal handler')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    helper = '/tmp/goal19-remote-exec-signals-' + str(time.time_ns())
    (args.output / 'session.json').write_text(json.dumps({'helper': helper}, indent=2) + '\n')
    with contextlib.closing(paramiko.Agent()) as agent:
        with connect(args, agent.get_keys()[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.put(args.binary, helper)
            sftp.chmod(helper, 0o755)
            argv = ['/usr/bin/locally', helper, args.peer, args.owner, f'/wki/{args.owner}' + helper]
            if args.handler:
                argv.append('--handler')
            command = shlex.join(argv)
            status, output, error = run(transport, command)
            (args.output / 'stdout.txt').write_text(output + '\n')
            (args.output / 'stderr.txt').write_text(error)
            (args.output / 'result.json').write_text(json.dumps({'command': command, 'status': status}, indent=2) + '\n')
            print(output, flush=True)
            rows = [line for line in output.splitlines() if line.startswith(('PASS ', 'FAIL '))]
            if len(rows) != 4 or any(not row.endswith('reaped=1') for row in rows):
                raise RuntimeError('fixture requires child lifetime review before cleanup: ' + helper)
            sftp.remove(helper)
            print('fixture cleanup completed', flush=True)
            if status or error or any(row.startswith('FAIL ') for row in rows):
                raise SystemExit(1)


if __name__ == '__main__':
    main()
