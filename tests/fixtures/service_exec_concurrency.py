#!/usr/bin/env python3
"""Run exec concurrency cases in a disposable WOS VM."""
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
    path = '/tmp/goal19-exec-concurrency-' + str(time.time_ns())
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError('authorized SSH agent key required')
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            try:
                sftp.put(args.binary, path)
                sftp.chmod(path, 0o755)
                sftp.put(args.binary, path + '-payload')
                sftp.chmod(path + '-payload', 0o755)
                status, output, error = run(transport, shlex.quote(path) + ' ' + shlex.quote(path + '-payload'))
                print(output, flush=True)
                if status or error or output.count('PASS profile=') != 78 or 'PASS: 78 native exec-concurrency cases' not in output:
                    raise RuntimeError(f'exec concurrency: status={status} stderr={error!r}')
            finally:
                sftp.remove(path + '-payload')
                sftp.remove(path)
                print('fixture cleanup completed', flush=True)


if __name__ == '__main__':
    main()
