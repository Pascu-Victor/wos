#!/usr/bin/env python3
"""Run native statvfs caller/output checks through WOS libc on TMPFS and XFS."""
import argparse
import contextlib
import json
from pathlib import Path
import re
import shlex
import time

import paramiko
from service_mmap_policy import run
from service_ssh_broker_lifetime import connect


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(exist_ok=False, parents=True)
    suffix = str(time.time_ns())
    image = '/tmp/goal19-statvfs-' + suffix
    results = []
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError('authorized SSH agent key required')
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.get_channel().settimeout(30)
            sftp.put(args.binary, image)
            try:
                sftp.chmod(image, 0o755)
                for backend, root in [('tmpfs', '/tmp'), ('xfs', '')]:
                    base = root + '/goal19-statvfs-files-' + suffix
                    status, stdout, stderr = run(transport, shlex.join(['/usr/bin/locally', image, base]))
                    (args.output / (backend + '.txt')).write_text(stdout)
                    cases = re.findall(r'^(PASS|FAIL) profile=(\d+) case=(\d+) errno=(\d+) expected=(\d+)$', stdout, re.M)
                    summaries = re.findall(r'^SUMMARY profile=(\d+) checks=(\d+) failures=(\d+)$', stdout, re.M)
                    okay = status == 0 and not stderr and len(cases) == 22 and all(row[0] == 'PASS' for row in cases) and \
                        summaries == [('0', '11', '0'), ('1', '11', '0')] and 'PASS statvfs checks=22 cleanup=1' in stdout
                    result = {'backend': backend, 'status': status, 'stderr': stderr, 'checks': len(cases), 'passed': okay}
                    results.append(result)
                    (args.output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
                    print(json.dumps(result), flush=True)
                    if not okay:
                        raise RuntimeError('statvfs functional checks failed; inspect saved output')
            finally:
                sftp.remove(image)
    print('PASS: 44 native statvfs checks; fixture cleanup completed', flush=True)


if __name__ == '__main__':
    main()
