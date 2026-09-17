#!/usr/bin/env python3
"""Signal one owned process group through B, home A, and B again."""
import argparse
import contextlib
import json
from pathlib import Path
import re
import shlex
import time

import paramiko
from service_ssh_broker_lifetime import connect


def run(transport, command, progress=None):
    with transport.open_session(timeout=10) as channel:
        # The native parent owns a 90-second alarm and bounded child cleanup.
        channel.settimeout(120)
        channel.exec_command(command)
        output = bytearray()
        while chunk := channel.recv(4096):
            output.extend(chunk)
            if progress is not None:
                with progress.open('ab') as stream:
                    stream.write(chunk)
            if len(output) > 32768:
                raise RuntimeError('fixture output bound')
        error = channel.recv_stderr(4096).decode()
        return channel.recv_exit_status(), output.decode(), error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--peer', default='wos-3')
    parser.add_argument('--owner', default='wos-0')
    parser.add_argument('--binary', required=True)
    parser.add_argument('--mapping', action='store_true', help='B trusts nonroot IDs; root parent signals a UID100 child')
    parser.add_argument('--uid', type=int, choices=(0, 100), help='restrict the ordinary matrix to one identity')
    parser.add_argument('--third', help='execute the middle stage on this third node instead of returning home')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.mapping and args.uid is not None:
        parser.error('--mapping already selects the UID100 child')
    if args.third and (args.third in (args.peer, args.owner) or not (args.mapping or args.uid == 100)):
        parser.error('--third needs a distinct node and --mapping or --uid 100')
    args.output.mkdir(parents=True, exist_ok=False)
    helper = '/tmp/goal19-group-return-' + str(time.time_ns())
    records = []
    (args.output / 'session.json').write_text(json.dumps({'helper': helper}, indent=2) + '\n')
    with contextlib.closing(paramiko.Agent()) as agent:
        with connect(args, agent.get_keys()[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.put(args.binary, helper)
            sftp.chmod(helper, 0o755)
            identities = (100,) if args.mapping else ((args.uid,) if args.uid is not None else (0, 100))
            for uid in identities:
                for executor in ('main', 'worker'):
                    directory = helper + f'-{uid}-{executor}'
                    sftp.mkdir(directory, 0o777)
                    sftp.chmod(directory, 0o777)
                    output_dir = args.output / f'{uid}-{executor}'
                    output_dir.mkdir()
                    command = shlex.join(['/usr/bin/locally', helper, str(uid), '/wki/' + args.owner + directory,
                                          '/wki/' + args.owner + helper, args.peer, args.owner, executor,
                                          *(['--mapping'] if args.mapping else (['--third'] if args.third else [])),
                                          *([args.third] if args.third else [])])
                    record = {'uid': uid, 'executor': executor, 'directory': directory, 'command': command,
                              'started_ns': time.time_ns()}
                    records.append(record)
                    (args.output / 'results.json').write_text(json.dumps(records, indent=2) + '\n')
                    status, output, error = run(transport, command, output_dir / 'stdout.txt')
                    record.update(status=status, ended_ns=time.time_ns())
                    (output_dir / 'stdout.txt').write_text(output)
                    (output_dir / 'stderr.txt').write_text(error)
                    print(output, end='', flush=True)
                    for leaf in sftp.listdir(directory):
                        if not re.fullmatch(r'[123]-(ready|observed|advance|failure)', leaf):
                            raise RuntimeError('unexpected fixture marker: ' + leaf)
                        with sftp.open(directory + '/' + leaf, 'rb') as stream:
                            data = stream.read(4096)
                        (output_dir / (leaf + '.bin')).write_bytes(data)
                    (args.output / 'results.json').write_text(json.dumps(records, indent=2) + '\n')
                    policies = re.findall(r'^PASS policy stage=([123]) .*sender_reaped=1$', output, re.MULTILINE)
                    if args.third:
                        complete = re.search(r'^(PASS|FAIL) complete .*status=5888 normal=1 reaped=1$', output, re.MULTILINE)
                        observations = re.findall(r'^(PASS|FAIL) stage=([123]) receiver=(\d+) host=(\S+) pgid=\d+ uid=100/100/100 query=0 query_errno=0 signal=0 signal_errno=0 observed=1 delivered=1 identity=1$', output, re.MULTILINE)
                        if status not in (0, 1) or error or not complete or [row[1] for row in observations] != ['1', '2', '3']:
                            raise RuntimeError('third-node lifetime or transport requires review: ' + directory)
                        record['behavior_passed'] = status == 0 and complete[1] == 'PASS' and all(row[0] == 'PASS' for row in observations) and (not args.mapping or policies == ['1', '2', '3'])
                        stages = [(row[1], row[2], row[3]) for row in observations]
                        if args.mapping:
                            probes = re.findall(r'^RECEIVER-QUERY host=(\S+) caller=(\d+) target=(\d+) uid=0/0/0 result=0 errno=0$', output, re.MULTILINE)
                            if len(probes) != 1 or probes[0][0] != args.third or probes[0][2] != stages[1][1] or probes[0][1] == probes[0][2]:
                                raise RuntimeError('direct receiver permission control requires review: ' + directory)
                            record['receiver_probe'] = {'host': probes[0][0], 'pid': probes[0][1], 'target': probes[0][2]}
                    else:
                        if not re.search(r'^PASS complete .*normal=1 reaped=1$', output, re.MULTILINE):
                            raise RuntimeError('fixture completion requires review: ' + directory)
                        stages = re.findall(r'^PASS stage=([123]) receiver=(\d+) host=(\S+) .*delivered=1 identity=1$', output, re.MULTILINE)
                        if status or error or [row[0] for row in stages] != ['1', '2', '3']:
                            raise RuntimeError('fixture behavior requires review: ' + directory)
                        if args.mapping and policies != ['1', '2', '3']:
                            raise RuntimeError('fixture mapping requires review: ' + directory)
                        record['behavior_passed'] = True
                    record['retirement'] = []
                    for stage, pid, host in stages:
                        expected = (args.third or args.owner) if stage == '2' else args.peer
                        if host != expected:
                            raise RuntimeError('wrong stage host')
                        prefix = ['/usr/bin/locally'] if host == args.owner else ['/usr/bin/on', host]
                        body = 'test -d /proc/$$ || exit 3; test ! -d /proc/' + pid + ' || exit 4'
                        command = shlex.join([*prefix, '/usr/bin/sh', '-c', body])
                        if host != args.owner and uid == 100:
                            command = shlex.join(['/usr/bin/locally', helper, '--peer-retired', host, pid])
                        retired, out, err = run(transport, command)
                        record['retirement'].append({'stage': stage, 'pid': pid, 'host': host, 'status': retired,
                                                     'stdout': out, 'stderr': err})
                        (args.output / 'results.json').write_text(json.dumps(records, indent=2) + '\n')
                        if retired or out or err:
                            raise RuntimeError('receiver retirement requires review: ' + directory)
                    if 'receiver_probe' in record:
                        probe = record['receiver_probe']
                        body = 'test -d /proc/$$ || exit 3; test ! -d /proc/' + probe['pid'] + ' || exit 4'
                        command = shlex.join(['/usr/bin/on', probe['host'], '/usr/bin/sh', '-c', body])
                        retired, out, err = run(transport, command)
                        probe.update(status=retired, stdout=out, stderr=err)
                        (args.output / 'results.json').write_text(json.dumps(records, indent=2) + '\n')
                        if retired or out or err:
                            raise RuntimeError('receiver query process retirement requires review')
                    for leaf in sftp.listdir(directory):
                        sftp.remove(directory + '/' + leaf)
                    sftp.rmdir(directory)
                    record['cleaned'] = True
                    (args.output / 'results.json').write_text(json.dumps(records, indent=2) + '\n')
            sftp.remove(helper)
    print(f'All {len(records)} source children normally reaped with exit23; all {len(records) * 3} stage receivers absent; fixture files removed.', flush=True)
    failed = [f"{row['uid']}-{row['executor']}" for row in records if not row['behavior_passed']]
    if failed:
        print('Policy matrix failures: ' + ', '.join(failed), flush=True)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
