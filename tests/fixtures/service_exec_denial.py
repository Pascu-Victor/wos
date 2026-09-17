#!/usr/bin/env python3
"""Historical baseline: run the pre-quiescence set-ID EAGAIN guard in a disposable WOS VM."""
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
    parser.add_argument('--worker', action='store_true', help='attempt denied exec from an additional worker')
    parser.add_argument('--legacy-eagain', action='store_true',
                        help='explicitly select a kernel predating Goal 19 set-ID exec quiescence')
    args = parser.parse_args()
    if not args.legacy_eagain:
        parser.error('historical fixture requires --legacy-eagain and an older kernel; use service_exec_nosuid.py for current set-ID behavior')
    path = '/tmp/goal19-exec-denial-' + str(time.time_ns())
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError('authorized SSH agent key required')
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            installed = []
            try:
                sftp.put(args.binary, path)
                installed.append(path)
                sftp.chmod(path, 0o755)
                images = [path + '-suid', path + '-sgid', path + '-both']
                for image, mode in zip(images, (0o4755, 0o2755, 0o6755)):
                    sftp.put(args.binary, image)
                    installed.append(image)
                    sftp.chown(image, 1001, 2002)
                    sftp.chmod(image, mode)
                status, output, error = run(transport, ' '.join(shlex.quote(p) for p in [path] + images + (['--worker'] if args.worker else [])))
                print(output, flush=True)
                if status or error or output.count('PASS profile=') != 30 or 'PASS: 30 native exec denial cases' not in output:
                    raise RuntimeError(f'exec denial: status={status} stderr={error!r}')
            finally:
                for image in reversed(installed):
                    sftp.remove(image)
                print('fixture cleanup completed', flush=True)


if __name__ == '__main__':
    main()
