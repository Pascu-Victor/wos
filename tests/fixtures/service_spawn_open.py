#!/usr/bin/env python3
"""Check spawn OPEN path authority and ordered file actions in a disposable WOS VM."""
import argparse
import contextlib
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
    parser.add_argument('--base-root', default='/tmp')
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--raw', action='store_true')
    modes.add_argument('--fallback', action='store_true')
    args = parser.parse_args()
    path = '/tmp/goal19-spawn-open-' + str(time.time_ns())
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError('authorized SSH agent key required')
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            try:
                sftp.put(args.binary, path)
                sftp.chmod(path, 0o755)
                status, output, error = run(transport, shlex.quote(path) + ' ' + shlex.quote(args.base_root.rstrip('/') + '/' + path.rsplit('/', 1)[-1] + '-files') + (' --raw' if args.raw else ' --fallback' if args.fallback else ''))
                print(output, flush=True)
                if status or error or output.count('PASS case=') != 24 or 'spawn OPEN cleanup completed' not in output:
                    raise RuntimeError(f'spawn OPEN: status={status} stderr={error!r}')
            finally:
                sftp.remove(path)
                print('fixture cleanup completed', flush=True)


if __name__ == '__main__':
    main()
