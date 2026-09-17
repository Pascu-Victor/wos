#!/usr/bin/env python3
"""Check running executable file-byte stability in a disposable WOS VM."""
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
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--write-only', action='store_true', help='only existing/new writer cases, also usable on WKI files')
    mode.add_argument('--remote-cases', action='store_true', help='all cases except unsupported remote hardlink creation')
    args = parser.parse_args()
    cases = 2 if args.write_only else (10 if args.remote_cases else 11)
    path = '/tmp/goal19-exec-file-stability-' + str(time.time_ns())
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError('authorized SSH agent key required')
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            try:
                sftp.put(args.binary, path)
                sftp.chmod(path, 0o755)
                base = args.base_root.rstrip('/') + '/' + path.rsplit('/', 1)[-1] + '-image'
                command = shlex.quote(path) + ' ' + shlex.quote(base)
                command += ' --write-only' if args.write_only else (' --remote-cases' if args.remote_cases else '')
                status, output, error = run(transport, command)
                print(output, flush=True)
                if status or error or output.count('PASS case=') != cases or output.count('case cleanup completed') != cases:
                    raise RuntimeError(f'exec file stability: status={status} stderr={error!r}')
            finally:
                base = args.base_root.rstrip('/') + '/' + path.rsplit('/', 1)[-1] + '-image'
                for which in range(2 if args.write_only else 11):
                    if args.remote_cases and which == 2:
                        continue
                    for suffix in ('', '-link'):
                        try:
                            sftp.remove(base + '-' + str(which) + suffix)
                        except OSError as error:
                            if error.errno != 2:
                                raise
                sftp.remove(path)
                print('fixture cleanup completed', flush=True)


if __name__ == '__main__':
    main()
