#!/usr/bin/env python3
"""Verify native exec argument snapshots and recursive shebang ordering in WOS."""
import argparse
import contextlib
import shlex
import shutil
import tempfile
import time
import paramiko
from service_ssh_broker_lifetime import connect
from service_wki_identity import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--binary', required=True)
    parser.add_argument('--large-image', action='store_true',
                        help='pad the ELF to 8 MiB to exercise file-backed loading')
    args = parser.parse_args()
    path = '/tmp/goal19-exec-arguments-' + str(time.time_ns())
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError('authorized SSH agent key required')
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            installed = []
            try:
                installed.append(path)
                if args.large_image:
                    with tempfile.TemporaryFile() as image, open(args.binary, 'rb') as source:
                        shutil.copyfileobj(source, image)
                        image.truncate(max(image.tell(), 8 * 1024 * 1024))
                        image.seek(0)
                        sftp.putfo(image, path)
                else:
                    sftp.put(args.binary, path)
                sftp.chmod(path, 0o755)
                for depth in range(1, 6):
                    script = path + f'.s{depth}'
                    interpreter = path if depth == 1 else path + f'.s{depth - 1}'
                    with sftp.open(script, 'w') as stream:
                        installed.append(script)
                        stream.write(f'#!{interpreter} arg{depth - 1}\n')
                    sftp.chmod(script, 0o755)
                status, output, error = run(transport, shlex.quote(path))
                print(output, flush=True)
                if status or error or output.count('PASS profile=') != 20:
                    raise RuntimeError(f'exec arguments: status={status} stderr={error!r}')
            finally:
                for name in reversed(installed):
                    sftp.remove(name)
                print('fixture cleanup completed', flush=True)


if __name__ == '__main__':
    main()
