#!/usr/bin/env python3
"""Check accepted credential-bearing exec retirement after owned receiver resets."""
import argparse
import contextlib
import json
from pathlib import Path
import re
import shlex
import subprocess
import time

import paramiko
from service_ssh_broker_lifetime import connect
from service_exec_peer_restart import restart_owner, snapshot, wait_snapshot


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--peer', default='wos-3')
    parser.add_argument('--node', type=int, default=3)
    parser.add_argument('--descriptor', type=Path, required=True)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--large-binary', required=True)
    parser.add_argument('--debug-dir', type=Path, required=True)
    parser.add_argument('--wosdbg', type=Path, required=True)
    parser.add_argument('--build-id', required=True)
    parser.add_argument('--sizes', nargs='+', choices=('small', 'large'), default=('small', 'large'))
    parser.add_argument('--profiles', nargs='+', choices=('root', 'inert', 'service'), default=('root', 'inert', 'service'))
    parser.add_argument('--workers', nargs='+', type=int, choices=(0, 1), default=(0, 1))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    helper = '/tmp/goal19-running-exec-restart-' + str(time.time_ns())
    results = []
    (args.output / 'session.json').write_text(json.dumps({'helper': helper}, indent=2) + '\n')

    def verify(label):
        with (args.output / (label + '-debug.json')).open('w') as out, (args.output / (label + '-debug.stderr')).open('w') as err:
            check = subprocess.run([str(args.wosdbg.resolve()), '--batch', 'verify-live.json'], cwd=args.debug_dir,
                                   stdout=out, stderr=err, timeout=30)
        if check.returncode:
            raise RuntimeError('live debugger verification failed')
        result = json.loads((args.output / (label + '-debug.json')).read_text())
        calls = {c['id']: c['result'] for c in result['calls']}
        if not result['ok'] or not all(c['ok'] for c in calls.values()) or not calls['close']['closed']:
            raise RuntimeError('debugger verification/session close failed')
        for target in calls['state']['targets']:
            if target['localBuildId'] != args.build_id or target['remoteBuildId'] != args.build_id or target['symbolsQuarantined']:
                raise RuntimeError('live kernel identity mismatch')
        if not all(calls[node]['symbolsVerified'] for node in ('node0', 'node3')):
            raise RuntimeError('unverified live symbols')

    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError('authorized SSH agent key required')
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            for source, suffix in ((args.binary, ''), (args.large_binary, '-large')):
                sftp.put(source, helper + suffix)
                sftp.chmod(helper + suffix, 0o755)
            verify('initial')
            for size in args.sizes:
                for profile in args.profiles:
                    for worker in args.workers:
                        label = f'{size}-{profile}-{worker}'
                        before = snapshot(transport, args.output, label + '-before', args.peer)
                        payload = '/wki/wos-0' + helper + ('-large' if size == 'large' else '')
                        for mode in ('restart', 'control'):
                            with transport.open_session(timeout=10) as channel:
                                channel.settimeout(110)
                                command = shlex.join(['/usr/bin/locally', helper, args.peer, profile, str(worker), payload, mode])
                                channel.exec_command(command)
                                with channel.makefile('r') as reader, (args.output / (label + '-' + mode + '.log')).open('w') as log:
                                    def line():
                                        text = reader.readline()
                                        log.write(text)
                                        log.flush()
                                        return text.strip()
                                    admitted = line()
                                    match = re.fullmatch(r'ADMITTED child=(\d+) receiver=(\d+) exact=1', admitted)
                                    if match and mode == 'restart':
                                        reset = restart_owner(args.descriptor, args.node, args.peer)
                                        (args.output / (label + '-reset.json')).write_text(json.dumps(reset, indent=2) + '\n')
                                    final = line()
                                    status = channel.recv_exit_status()
                                    error = channel.recv_stderr(8192).decode()
                                    passed = bool(match) and final == (f'RESULT passed=1 child={match[1]} timely=1 reaped=1 '
                                                       'status_ok=1 parent_unchanged=1 consumed=1') and status == 0 and not error
                                    results.append({'label': label, 'mode': mode, 'command': command, 'admitted': admitted,
                                                    'final': final, 'status': status, 'stderr': error, 'passed': passed})
                                    (args.output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
                                    print(label, mode, final, flush=True)
                                    if not passed:
                                        raise RuntimeError('owned child lifetime/identity failure; artifacts retained')
                            if mode == 'restart':
                                after = wait_snapshot(transport, args.output, label + '-after', args.peer,
                                                      lambda s: s['epoch'] != 0 and s['epoch'] != before['epoch'] and
                                                      any(p['mount'] == '/wki/' + args.peer for p in s['active_proxies']))
                                (args.output / (label + '-epochs.json')).write_text(json.dumps({'before': before['epoch'], 'after': after['epoch']}, indent=2) + '\n')
                                verify(label)
            for suffix in ('', '-large'):
                sftp.remove(helper + suffix)
            (args.output / 'cleanup.json').write_text(json.dumps({'removed': [helper, helper + '-large']}, indent=2) + '\n')
            count = len(args.sizes) * len(args.profiles) * len(args.workers)
            print(f'PASS: {count} accepted-restart cases and {count} fresh controls; owned helpers removed', flush=True)


if __name__ == '__main__':
    main()
