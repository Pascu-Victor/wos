#!/usr/bin/env python3
"""Check local and remote signal permissions after a target changes credentials."""
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
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    helper = '/tmp/goal19-signal-identity-' + str(time.time_ns())
    (args.output / 'session.json').write_text(json.dumps({'helper': helper}, indent=2) + '\n')
    with contextlib.closing(paramiko.Agent()) as agent:
        with connect(args, agent.get_keys()[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.put(args.binary, helper)
            sftp.chmod(helper, 0o755)
            command = shlex.join(['/usr/bin/locally', helper, args.peer, f'/wki/{args.owner}' + helper])
            status, output, error = run(transport, command)
            (args.output / 'stdout.txt').write_text(output + '\n')
            (args.output / 'stderr.txt').write_text(error)
            (args.output / 'result.json').write_text(json.dumps({'command': command, 'status': status}, indent=2) + '\n')
            print(output, flush=True)
            rows = [line for line in output.splitlines() if line.startswith(('PASS ', 'FAIL '))]
            if len(rows) != 12 or any(not row.endswith('reaped=1') for row in rows):
                raise RuntimeError('fixture needs child-lifetime review before cleanup: ' + helper)
            sftp.remove(helper)
            print('fixture cleanup completed', flush=True)
            if status != 0 or error or any(row.startswith('FAIL ') for row in rows):
                raise SystemExit(1)


if __name__ == '__main__':
    main()
