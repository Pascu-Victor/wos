#!/usr/bin/env python3
"""Run hostname checks locally in a disposable WOS VM, preserving evidence."""
import argparse
import contextlib
from pathlib import Path
import time

import paramiko

from service_exec_output import collect
from service_ssh_broker_lifetime import connect


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    helper = '/tmp/goal19-hostname-' + str(time.time_ns())
    with contextlib.closing(paramiko.Agent()) as agent:
        with connect(args, agent.get_keys()[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            try:
                sftp.put(args.binary, helper)
                sftp.chmod(helper, 0o755)
                result, output = collect(transport, ['/usr/bin/locally', helper], args.output / 'hostname')
                print(output.decode(), flush=True)
                if result['status'] or result['stderr'] or b'failures=0 writes=24000 reads=48000' not in output:
                    raise RuntimeError(f'hostname check failed: {result}')
            finally:
                sftp.remove(helper)
                print('fixture cleanup completed', flush=True)


if __name__ == '__main__':
    main()
