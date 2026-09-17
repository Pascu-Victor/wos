#!/usr/bin/env python3
"""Check directory identity across ordinary exec after cwd rename/name reuse."""
import argparse
import contextlib
import json
from pathlib import Path
import re
import shlex
import time

import paramiko

from service_ssh_broker_lifetime import connect
from service_wki_identity import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--peer', default='wos-3')
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    suffix = str(time.time_ns())
    image = '/tmp/goal19-cwd-exec-' + suffix
    owner_image = '/wki/' + args.peer + image
    results = []
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError('authorized SSH agent key required')
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.get_channel().settimeout(30)
            uploaded = []
            try:
                # /tmp is receiver-local under the normal placement policy.
                # Provision both images so cwd selection is the only variable.
                for destination in (image, owner_image):
                    sftp.put(args.binary, destination)
                    uploaded.append(destination)
                    sftp.chmod(destination, 0o755)
                for backend, root in [('tmpfs', '/tmp'), ('xfs', '')]:
                    base = '/wki/' + args.peer + root + '/goal19-cwd-exec-directory-' + suffix
                    status, stdout, stderr = run(transport, shlex.join(['/usr/bin/locally', image, args.peer, base]))
                    (args.output / (backend + '.txt')).write_text(stdout + '\n')
                    cases = re.findall(r'^(PASS|FAIL) stage=(\S+) profile=(\d+) worker=(\d) remote=(\d) status=(\d+)$', stdout, re.M)
                    passed = status == 0 and not stderr and len(cases) == 9 and all(row[0] == 'PASS' for row in cases) and \
                        'PASS cwd exec identity checks=9 failed=0 cleanup=1' in stdout
                    results.append({'backend': backend, 'status': status, 'stderr': stderr, 'cases': cases, 'pass': passed})
                    (args.output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
                    print(backend + '\n' + stdout, flush=True)
            finally:
                for destination in reversed(uploaded):
                    sftp.remove(destination)
                print('uploaded images removed', flush=True)
    if len(results) != 2 or not all(row['pass'] for row in results):
        raise RuntimeError('retained cwd identity failed; see per-backend results')


if __name__ == '__main__':
    main()
