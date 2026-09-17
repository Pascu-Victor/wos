#!/usr/bin/env python3
"""Compare local and remote exec saved-ID and set-ID semantics on owned files."""
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
    parser.add_argument('--owner', default='wos-0')
    parser.add_argument('--binary', required=True)
    execution = parser.add_mutually_exclusive_group()
    execution.add_argument('--worker', action='store_true')
    execution.add_argument('--spawn', action='store_true')
    parser.add_argument('--extended', action='store_true')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(exist_ok=False, parents=True)
    directory = '/tmp/goal19-exec-ids-' + str(time.time_ns())
    helper = directory + '/launcher'
    seed = directory + '/seed'
    rows = []
    (args.output / 'session.json').write_text(json.dumps({'directory': directory}, indent=2) + '\n')
    with contextlib.closing(paramiko.Agent()) as agent:
        with connect(args, agent.get_keys()[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.get_channel().settimeout(30)
            sftp.mkdir(directory, 0o755)
            sftp.chmod(directory, 0o755)
            sftp.put(args.binary, helper)
            sftp.chmod(helper, 0o755)
            sftp.put(args.binary, seed)
            sftp.chown(seed, 1000, 2000)
            sftp.chmod(seed, 0o6755)
            profiles = [
                ('plain', 0, 0, 0o755, False, 1002, 2002),
                ('setuid', 1003, 0, 0o4755, False, 1003, 2002),
                ('setgid', 0, 2003, 0o2755, False, 1002, 2003),
                ('both', 1003, 2003, 0o6755, False, 1003, 2003),
                ('nnp', 1003, 2003, 0o6755, True, 1002, 2002),
            ]
            if args.extended:
                profiles += [
                    ('setuid_unreadable', 1003, 0, 0o4155, False, 1003, 2002),
                    ('setgid_unreadable', 0, 2003, 0o2715, False, 1002, 2003),
                    ('service', 0, 0, 0o755, True, 1002, 2002),
                    ('service_inert', 0, 0, 0o755, True, 1002, 2002),
                ]
            for profile, uid, gid, mode, nnp, eu, eg in profiles:
                payload = directory + '/' + profile
                sftp.put(args.binary, payload)
                sftp.chown(payload, uid, gid)
                sftp.chmod(payload, mode)
                observed = sftp.stat(payload)
                if (observed.st_uid, observed.st_gid, observed.st_mode & 0o7777) != (uid, gid, mode):
                    raise RuntimeError('payload ownership/mode not installed')
                for target, expected_host in (('local', args.owner), (args.peer, args.peer)):
                    image = payload if target == 'local' else '/wki/' + args.owner + payload
                    command = shlex.join(['/usr/bin/locally', helper, target, image, profile if profile.startswith('service') else 'nnp' if nnp else 'normal', str(eu), str(eg), expected_host, seed] + (['worker'] if args.worker else ['spawn'] if args.spawn else []))
                    status, output, error = run(transport, command)
                    row = {'profile': profile, 'target': target, 'status': status, 'stdout': output, 'stderr': error,
                           'expected_euid': eu, 'expected_egid': eg, 'worker': args.worker, 'spawn': args.spawn}
                    rows.append(row)
                    (args.output / 'results.json').write_text(json.dumps(rows, indent=2) + '\n')
                    print(profile, target, status, output, error, flush=True)
                    # A rejected spawn has no reported payload PID. Preserve the
                    # failure separately from executed-child retirement checks.
                    spawn_error = re.search(r'^SPAWN-ERROR errno=(\d+)$', output, re.M)
                    if args.spawn and status == 12 and not error and spawn_error and re.search(r'^COMPLETE .* reaped=1 status=3072$', output, re.M):
                        row.update(passed=False, spawn_errno=int(spawn_error[1]), source_reaped=True)
                        (args.output / 'results.json').write_text(json.dumps(rows, indent=2) + '\n')
                        continue
                    match = re.search(r'^(PASS|FAIL) host=(\S+) pid=(\d+) ', output, re.M)
                    if status not in (0, 1) or error or not match or match[2] != expected_host or not re.search(r'^COMPLETE .* reaped=1 status=(0|256)$', output, re.M):
                        raise RuntimeError('execution/retirement requires review; preserve fixture')
                    prefix = ['/usr/bin/locally'] if target == 'local' else ['/usr/bin/on', args.peer]
                    probe = shlex.join([*prefix, '/usr/bin/sh', '-c', 'test -d /proc/$$ || exit 3; test ! -d /proc/' + match[3] + ' || exit 4'])
                    retired, out, err = run(transport, probe)
                    row['retirement'] = {'status': retired, 'stdout': out, 'stderr': err}
                    row['passed'] = status == 0 and match[1] == 'PASS' and bool(re.search(r'^AUXV .* ids=1 ', output, re.M))
                    if args.spawn:
                        row['passed'] &= bool(re.search(r'^SPAWN-COMPLETE .* reaped=1 status=0$', output, re.M))
                    (args.output / 'results.json').write_text(json.dumps(rows, indent=2) + '\n')
                    if retired or out or err:
                        raise RuntimeError('owned receiver retirement requires review')
                sftp.remove(payload)
            sftp.remove(seed)
            sftp.remove(helper)
            sftp.rmdir(directory)
    failed = sum(not row['passed'] for row in rows)
    summary = {'cases': len(rows), 'failed': failed, 'cleanup_completed': True}
    (args.output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary), flush=True)
    raise SystemExit(int(failed != 0))


if __name__ == '__main__':
    main()
