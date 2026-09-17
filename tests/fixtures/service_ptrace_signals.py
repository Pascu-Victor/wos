#!/usr/bin/env python3
"""Check signal/ptrace interactions using owned children on two private WOS guests."""
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
    parser.add_argument('--hosts', nargs=2, default=['10.10.0.235', '10.10.0.232'])
    parser.add_argument('--username', default='root')
    parser.add_argument('--binary', required=True)
    parser.add_argument('--extended', action='store_true')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    helper = '/tmp/goal19-ptrace-signals-' + str(time.time_ns())
    receipts = []
    (args.output / 'session.json').write_text(json.dumps({'helper': helper, 'hosts': args.hosts}, indent=2) + '\n')
    with contextlib.closing(paramiko.Agent()) as agent:
        for host in args.hosts:
            args.host = host
            with connect(args, agent.get_keys()[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
                sftp.put(args.binary, helper)
                sftp.chmod(helper, 0o755)
                command = shlex.join(['/usr/bin/locally', helper] + (['--extended'] if args.extended else []))
                status, output, error = run(transport, command)
                (args.output / (host + '-stdout.txt')).write_text(output + '\n')
                (args.output / (host + '-stderr.txt')).write_text(error)
                rows = [line for line in output.splitlines() if line.startswith(('PASS ', 'FAIL '))]
                controllers = [line for line in output.splitlines() if line.startswith('controller reaped=')]
                lifetime_ok = (len(rows) == 12 and len(controllers) == 12
                               and all(line.endswith('reaped=1 cleaned=1') for line in rows)
                               and all(line.startswith('controller reaped=1 ') for line in controllers))
                receipt = {'host': host, 'command': command, 'status': status, 'rows': rows,
                           'controllers': controllers, 'stderr': error, 'lifetime_ok': lifetime_ok, 'cleanup': False}
                if args.extended:
                    receipt['queued_stops'] = [line for line in output.splitlines() if line.startswith('queued-stop ')]
                    receipt['denials'] = [line for line in output.splitlines() if line.startswith('denial ')]
                    if (receipt['denials'] != ['denial reaped=1 status=0 preserved=1'] * 2
                            or receipt['queued_stops'] != ['queued-stop consumed=1'] * 2):
                        receipt['lifetime_ok'] = lifetime_ok = False
                receipts.append(receipt)
                (args.output / 'results.json').write_text(json.dumps(receipts, indent=2) + '\n')
                print(host, output, flush=True)
                if not lifetime_ok:
                    raise RuntimeError('Owned-child lifetime review required before fixture removal: ' + helper)
                sftp.remove(helper)
                receipt['cleanup'] = True
                (args.output / 'results.json').write_text(json.dumps(receipts, indent=2) + '\n')
                print(host, 'fixture cleanup completed', flush=True)
    if any(r['status'] != 0 or r['stderr'] or any(row.startswith('FAIL ') for row in r['rows']) for r in receipts):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
