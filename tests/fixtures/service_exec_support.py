#!/usr/bin/env python3
"""Verify remote exec refusal against a peer without executable-view adverts."""
import argparse
import contextlib
import json
from pathlib import Path
import time

import paramiko
from service_ssh_broker_lifetime import connect
from service_exec_cancellation import peer_identity, row, run, snapshot


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--peer', default='wos-3')
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    helper = '/tmp/goal19-exec-support-' + str(time.time_ns())
    results = []
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError('authorized SSH agent key required')
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.put(args.binary, helper)
            sftp.chmod(helper, 0o755)
            try:
                for backend, base in [('tmpfs', f'/wki/{args.peer}/tmp'), ('xfs', f'/wki/{args.peer}')]:
                    path = base + '/' + helper.rsplit('/', 1)[-1]
                    try:
                        before, peers = snapshot(transport, output, backend + '-before')
                        identity = peer_identity(peers, args.peer)
                        diag = (output / (backend + '-before-netdiag.txt')).read_text()
                        resources = [dict(token.split('=', 1) for token in line.split()[1:])
                                     for line in diag.splitlines() if line.startswith('wki_resource ')]
                        resources = [r for r in resources if r['kind'] == 'discovered' and r['type'] == 'vfs'
                                     and int(r['node'], 16) == int(identity[1]) and r['valid'] == '1']
                        if not resources or any(int(r['flags']) & 0x20 for r in resources):
                            raise RuntimeError('fixture requires live VFS adverts without the support bit')
                        # Filter on the owner to stay within remote-command
                        # capture limits; retain the kernel completeness marker.
                        owner_command = ['/usr/bin/on', args.peer, '/usr/bin/grep', '-E',
                                         '^(wki_vfs_exec |wki_netdiag_end )', '/proc/wki/netdiag']
                        owner_before = run(transport, owner_command)
                        (output / (backend + '-owner-before.txt')).write_text(owner_before)
                        started = time.monotonic()
                        text = run(transport, [helper, path, '--unsupported-peer'])
                        elapsed = time.monotonic() - started
                        (output / (backend + '-fixture.txt')).write_text(text)
                        if text.count('PASS unsupported ') != 1 or elapsed >= 10:
                            raise RuntimeError('unsupported exec must promptly deny and leave ordinary I/O usable')
                        after, peers_after = snapshot(transport, output, backend + '-after')
                        if before != after or peer_identity(peers_after, args.peer) != identity:
                            raise RuntimeError('unsupported exec changed cancellation state or peer identity')
                        owner_after = run(transport, owner_command)
                        (output / (backend + '-owner-after.txt')).write_text(owner_after)
                        if any(not d.rstrip().endswith('wki_netdiag_end complete=1') for d in (owner_before, owner_after)):
                            raise RuntimeError('incomplete owner diagnostic snapshot')
                        # The remote diagnostic command can itself execute an
                        # origin-host image, advancing the owner's client totals.
                        # Check retained state and owner pins after it returns.
                        owner_start = row(owner_before, 'wki_vfs_exec')
                        owner_end = row(owner_after, 'wki_vfs_exec')
                        if any(owner_start[key] != owner_end[key] for key in ('owned', 'pending', 'retrying', 'owner_views')):
                            raise RuntimeError('unsupported exec changed owner view state')
                        result = {'backend': backend, 'seconds': elapsed, 'pass': True,
                                  'consumer': after, 'resources': resources}
                        results.append(result)
                        print(json.dumps(result), flush=True)
                    finally:
                        try:
                            sftp.remove(path)
                        except OSError as error:
                            if error.errno != 2:
                                raise
            finally:
                sftp.remove(helper)
                print('fixture cleanup completed', flush=True)
    (output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')


if __name__ == '__main__':
    main()
