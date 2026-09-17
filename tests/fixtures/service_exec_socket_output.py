#!/usr/bin/env python3
"""Check inherited TCP write/close/half-close across local and remote exec."""
import argparse
import contextlib
import json
from pathlib import Path
import time

import paramiko

from service_exec_output import collect
from service_ssh_broker_lifetime import connect


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--peer', default='wos-3')
    parser.add_argument('--owner', default='wos-0')
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--sizes', type=int, nargs='+', default=[4096, 131072, 1048576])
    parser.add_argument('--uids', type=int, nargs='+', choices=[0, 100], default=[0, 100])
    parser.add_argument('--apis', nargs='+', choices=['write', 'send'], default=['write', 'send'])
    parser.add_argument('--endings', nargs='+', choices=['close', 'shutdown', 'shutdown-alias', 'shutdown-invalid', 'shutdown-read', 'inherited', 'close-wait'], default=['close', 'shutdown'])
    parser.add_argument('--hold-ms', type=int, choices=[0, 1000], default=0)
    args = parser.parse_args()
    if any(size <= 0 or size > 1048576 for size in args.sizes):
        parser.error('--sizes must be between 1 and 1048576')
    args.output.mkdir(parents=True, exist_ok=False)
    helper = '/tmp/goal19-socket-output-' + str(time.time_ns())
    (args.output / 'session.json').write_text(json.dumps({
        'helper': helper, 'peer': args.peer, 'owner': args.owner, 'hold_ms': args.hold_ms,
        'sizes': args.sizes, 'uids': args.uids, 'apis': args.apis, 'endings': args.endings,
    }, indent=2) + '\n')
    results = []
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError('authorized SSH agent key required')
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.put(args.binary, helper)
            sftp.chmod(helper, 0o755)
            try:
                for uid in args.uids:
                    for size in args.sizes:
                        for api in args.apis:
                            for ending in args.endings:
                                for target in ['local', args.peer]:
                                    case = f'{target}-{uid}-{size}-{api}-{ending}'
                                    command = ['/usr/bin/locally', helper, target, str(size), api, ending, str(uid), str(args.hold_ms)]
                                    started = time.monotonic()
                                    result, data = collect(transport, command, args.output / case)
                                    result['elapsed_seconds'] = round(time.monotonic() - started, 3)
                                    result['case'] = case
                                    output = data.decode()
                                    result['stdout'] = output
                                    expected_host = args.owner if target == 'local' else args.peer
                                    expected_stderr = f'SOCKET_ACCEPTED host={expected_host} uid={uid} bytes={size}\n'
                                    expected = (f'SOCKET_RESULT target={target} uid={uid} api={api} end={ending} requested={size} '
                                                f'received={size} exact=1 eof=1 replied=1 read_errno=0 status=0 passed=1\n')
                                    if ending in ['inherited', 'close-wait']:
                                        expected = f'SOCKET_LIFECYCLE end={ending} released=1 late={int(ending == "inherited")}\n' + expected
                                    result['passed'] = result['status'] == 0 and output == expected and result['stderr'] == expected_stderr
                                    results.append(result)
                                    (args.output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
                                    print('PASS' if result['passed'] else 'FAIL', case, json.dumps(result), flush=True)
            finally:
                sftp.remove(helper)
                print('fixture cleanup completed', flush=True)
    passed = sum(row['passed'] for row in results)
    print(f'SOCKET_MATRIX passed={passed} failed={len(results) - passed}', flush=True)
    if passed != len(results):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
