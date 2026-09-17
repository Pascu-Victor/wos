#!/usr/bin/env python3
"""Inherited stream receive, O_NONBLOCK and concurrent-reader checks on WOS."""
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
    parser.add_argument('--uids', type=int, nargs='+', choices=[0, 100], default=[0, 100])
    parser.add_argument('--readers', type=int, nargs='+', choices=[1, 4, 40], default=[1, 4, 40])
    parser.add_argument('--apis', nargs='+', choices=['read', 'recv'], default=['read', 'recv'])
    parser.add_argument('--modes', nargs='+', choices=['block', 'nonblock'], default=['block', 'nonblock'])
    parser.add_argument('--sizes', type=int, nargs='+', default=[4096, 131072, 1048576])
    args = parser.parse_args()
    if any(size <= 0 or size > 1048576 for size in args.sizes):
        parser.error('sizes must be 1..1048576')
    args.output.mkdir(parents=True, exist_ok=False)
    helper = '/tmp/goal19-socket-input-' + str(time.time_ns())
    (args.output / 'session.json').write_text(json.dumps({
        'helper': helper, 'peer': args.peer, 'owner': args.owner,
        'sizes': args.sizes, 'readers': args.readers, 'uids': args.uids, 'apis': args.apis, 'modes': args.modes,
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
                    for readers in args.readers:
                        for size in (args.sizes if readers == 1 else [128]):
                            for api in args.apis:
                                for mode in args.modes:
                                    for target in ['local', args.peer]:
                                        case = f'{target}-{uid}-{size}-{api}-{mode}-{readers}'
                                        command = ['/usr/bin/locally', helper, target, str(size), api, str(readers), str(uid), mode]
                                        started = time.monotonic()
                                        result, data = collect(transport, command, args.output / case)
                                        result.update(case=case, stdout=data.decode(), elapsed_seconds=round(time.monotonic() - started, 3))
                                        host = args.owner if target == 'local' else args.peer
                                        stderr = f'SOCKET_INPUT host={host} uid={uid} bytes={size} readers={readers} eof={readers} exact=1\n'
                                        stdout = f'SOCKET_INPUT_RESULT target={target} uid={uid} api={api} mode={mode} bytes={size} readers={readers} status=0 passed=1\n'
                                        result['passed'] = result['status'] == 0 and result['stdout'] == stdout and result['stderr'] == stderr
                                        results.append(result)
                                        (args.output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
                                        print('PASS' if result['passed'] else 'FAIL', case, json.dumps(result), flush=True)
                                        if not result['passed']:
                                            raise RuntimeError(f'failed receive case preserved: {case}')
            finally:
                sftp.remove(helper)
                print('fixture cleanup completed', flush=True)
    passed = sum(row['passed'] for row in results)
    print(f'SOCKET_INPUT_MATRIX passed={passed} failed={len(results) - passed}', flush=True)
    if passed != len(results):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
