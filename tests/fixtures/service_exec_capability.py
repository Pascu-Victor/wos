#!/usr/bin/env python3
"""Ordinary exec/spawn placement across advertised receiver versions."""
import argparse
import contextlib
import errno
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
    parser.add_argument('--binary', required=True)
    parser.add_argument('--compatible', action='store_true')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    directory = '/tmp/goal19-exec-capability-' + str(time.time_ns())
    helper = directory + '/helper'
    payload = directory + '/payload'
    rows = []
    (args.output / 'session.json').write_text(json.dumps({'directory': directory}, indent=2) + '\n')
    with contextlib.closing(paramiko.Agent()) as agent:
        with connect(args, agent.get_keys()[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.get_channel().settimeout(30)
            sftp.mkdir(directory, 0o755)
            sftp.chmod(directory, 0o755)
            for path in [helper, payload]:
                sftp.put(args.binary, path)
                sftp.chmod(path, 0o755)
            for profile in ['plain', 'service', 'inert']:
                for mode in ['exec', 'worker', 'spawn']:
                    for placement in ['strict', 'fallback']:
                        rejected = not args.compatible and placement == 'strict'
                        expected_host = 'wos-3' if args.compatible else 'wos-0'
                        marker = directory + '/marker'
                        with sftp.open(marker, 'w'):
                            pass
                        sftp.chown(marker, 1002, 2002)
                        sftp.chmod(marker, 0o600)
                        # /tmp may route to the receiver; name this owned source image explicitly.
                        command = shlex.join(['/usr/bin/locally', helper, 'wos-3', '/wki/wos-0' + payload, mode, placement,
                                              expected_host, str(errno.ENOTSUP if rejected else 0), profile, marker])
                        status, output, error = run(transport, command)
                        with sftp.open(marker, 'r') as stream:
                            observed = stream.read(3)
                        row = {'profile': profile, 'mode': mode, 'placement': placement, 'status': status,
                               'stdout': output, 'stderr': error, 'marker': observed.decode(errors='replace')}
                        rows.append(row)
                        (args.output / 'results.json').write_text(json.dumps(rows, indent=2) + '\n')
                        print(profile, mode, placement, status, output, error, flush=True)
                        completion = re.search(r'^COMPLETE child=(\d+) reaped=1 exit=(\d+) no_children=1$', output, re.M)
                        reports = re.findall(r'^PAYLOAD host=(\S+) pid=(\d+) identity=1 auxv=1 cloexec=1 marker=1$', output, re.M)
                        refusal = re.search(r'^REFUSAL errno=' + str(errno.ENOTSUP) + r' unchanged=1 cloexec=1 no_children=1 child_unchanged=1$', output, re.M)
                        passed = status == 0 and not error and completion is not None
                        if rejected:
                            passed &= bool(refusal) and not reports and observed == b'' and completion is not None and completion[2] == '0'
                        else:
                            passed &= len(reports) == 1 and reports[0][0] == expected_host and observed == b'x'
                            passed &= completion is not None and completion[2] == '23'
                            if mode == 'spawn':
                                passed &= bool(re.search(r'^SPAWN child=\d+ reaped=1 unchanged=1 no_children=1$', output, re.M))
                        row['passed'] = bool(passed)
                        if not passed:
                            (args.output / 'results.json').write_text(json.dumps(rows, indent=2) + '\n')
                            raise RuntimeError('execution/denial requires review; preserve owned fixture')
                        checks = [('wos-0', completion[1])]
                        if reports:
                            checks.append(reports[0])
                        for host, pid in checks:
                            prefix = ['/usr/bin/locally'] if host == 'wos-0' else ['/usr/bin/on', host]
                            probe = shlex.join([*prefix, '/usr/bin/sh', '-c', 'test -d /proc/$$ || exit 3; test ! -d /proc/' + pid + ' || exit 4'])
                            gone, out, err = run(transport, probe)
                            if gone or out or err:
                                raise RuntimeError('reported process retirement requires review')
                        row['retired'] = True
                        (args.output / 'results.json').write_text(json.dumps(rows, indent=2) + '\n')
                        sftp.remove(marker)
            sftp.remove(payload)
            sftp.remove(helper)
            sftp.rmdir(directory)
    summary = {'cases': len(rows), 'failed': sum(not x['passed'] for x in rows), 'cleanup_completed': True}
    (args.output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary), flush=True)


if __name__ == '__main__':
    main()
