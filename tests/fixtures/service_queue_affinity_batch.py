#!/usr/bin/env python3
"""Check denial atomicity on a disposable four-CPU VM's four-queue virtio NIC."""
import argparse
import contextlib
import json
from pathlib import Path
import shlex
import time

import paramiko
from service_ssh_broker_lifetime import connect
from service_wki_identity import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--interface', default='eth0')
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    helper = '/tmp/goal19-queue-affinity-batch-' + str(time.time_ns())
    (args.output / 'session.json').write_text(json.dumps({'helper': helper}, indent=2) + '\n')
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError('authorized SSH agent key required')
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.put(args.binary, helper)
            sftp.chmod(helper, 0o755)
            command = shlex.join(['/usr/bin/locally', helper, args.interface])
            status, out, err = run(transport, command)
            (args.output / 'stdout.txt').write_text(out + '\n')
            (args.output / 'stderr.txt').write_text(err)
            (args.output / 'result.json').write_text(json.dumps({'command': command, 'status': status, 'stderr': err}, indent=2) + '\n')
            print(out, flush=True)
            if 'restored=1' not in out or err:
                raise RuntimeError('owned fixture needs lifetime/restoration review: ' + helper)
            sftp.remove(helper)
            (args.output / 'cleanup.json').write_text(json.dumps({'removed': helper}, indent=2) + '\n')
            raise SystemExit(status)


if __name__ == '__main__':
    main()
