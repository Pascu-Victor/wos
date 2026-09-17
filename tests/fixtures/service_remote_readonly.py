#!/usr/bin/env python3
"""Run ordinary readonly-client checks on a disposable two-node WOS cluster."""
import argparse
import contextlib
import re
import shlex
import time
import paramiko
from service_ssh_broker_lifetime import connect
from service_wki_identity import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--owner', default='wos-3')
    parser.add_argument('--username', default='root')
    parser.add_argument('--binary', required=True)
    args = parser.parse_args()
    destination = '/tmp/goal19-remote-readonly-' + str(time.time_ns())
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError('an authorized SSH agent key is required')
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.get_channel().settimeout(30)
            sftp.put(args.binary, destination)
            sftp.chmod(destination, 0o755)
            try:
                status, output, error = run(transport, shlex.join(['/usr/bin/locally', destination, args.owner]))
                print(output, flush=True)
                profiles = re.findall(r'PROFILE (\d+) checks=(\d+) failed=(\d+)', output)
                if status or error or 'FAIL' in output or profiles != [('0', '33', '0'), ('1', '33', '0'), ('2', '33', '0')]:
                    raise RuntimeError(f'fixture status={status}, profiles={profiles}, stderr={error!r}')
                if 'PASS remote readonly fixture; cleanup completed' not in output:
                    raise RuntimeError('missing fixture cleanup receipt')
            finally:
                sftp.remove(destination)
                print('uploaded fixture removed', flush=True)


if __name__ == '__main__':
    main()
