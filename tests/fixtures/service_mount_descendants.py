#!/usr/bin/env python3
"""Run native mount descendant functional checks in a disposable WOS VM."""
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
    args = parser.parse_args()
    path = '/tmp/goal19-mount-descendants-' + str(time.time_ns())
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError('authorized SSH agent key required')
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            uploaded = False
            try:
                sftp.put(args.binary, path)
                uploaded = True
                sftp.chmod(path, 0o755)
                status, output, error = run(transport, '/usr/bin/locally ' + shlex.quote(path))
                print(output, flush=True)
                if status or error or output.count('PASS ') != 19 or 'FAIL' in output or \
                        'PASS: 19 native mount descendant checks; cleanup completed' not in output:
                    raise RuntimeError(f'native mount descendant: status={status} stderr={error!r}')
            finally:
                if uploaded:
                    sftp.remove(path)
                print('fixture cleanup completed', flush=True)


if __name__ == '__main__':
    main()
