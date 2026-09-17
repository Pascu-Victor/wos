#!/usr/bin/env python3
"""Run nosuid mount exec cases in a disposable WOS VM."""
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
    parser.add_argument('--remote-source', help='mount a disposable WKI export, for example wki://wos-3//tmp')
    args = parser.parse_args()
    path = '/tmp/goal19-exec-nosuid-' + str(time.time_ns())
    remote_paths = []
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError('authorized SSH agent key required')
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            try:
                sftp.put(args.binary, path)
                sftp.chmod(path, 0o755)
                command = shlex.quote(path) + ' ' + shlex.quote(path + '-mounts')
                if args.remote_source:
                    if not args.remote_source.startswith('wki://'):
                        raise ValueError('remote source must be a WKI URI')
                    peer, export = args.remote_source[6:].split('/', 1)
                    if export != '/tmp':
                        raise ValueError('this fixture requires the peer /tmp export')
                    prefix = path.rsplit('/', 1)[1]
                    for bits, mode in ((1, '4755'), (2, '2755'), (3, '6755')):
                        owner_path = '/tmp/' + prefix + '-image' + str(bits)
                        remote_path = '/wki/' + peer + owner_path
                        remote_paths.append(remote_path)
                        sftp.put(args.binary, remote_path)
                        status, output, error = run(transport, '/usr/bin/on ' + shlex.quote(peer) +
                                                    ' /usr/bin/chmod ' + mode + ' ' + shlex.quote(owner_path))
                        if status or error:
                            raise RuntimeError(f'owner mode setup: status={status} output={output!r} stderr={error!r}')
                    command += ' ' + shlex.quote(args.remote_source) + ' ' + shlex.quote(prefix)
                status, output, error = run(transport, command)
                print(output, flush=True)
                if status or error or output.count('PASS nosuid=') != 72 or output.count('PASS mount=') != 2 or 'PASS: 72 native nosuid exec cases' not in output or 'mount fixture cleanup completed' not in output:
                    raise RuntimeError(f'nosuid exec: status={status} stderr={error!r}')
            finally:
                for remote_path in remote_paths:
                    try:
                        sftp.remove(remote_path)
                    except FileNotFoundError:
                        pass
                sftp.remove(path)
                print('fixture cleanup completed', flush=True)


if __name__ == '__main__':
    main()
