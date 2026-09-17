#!/usr/bin/env python3
"""Signal a local group whose process was born on a nonroot-trusting peer."""
import argparse
import contextlib
import json
from pathlib import Path
import re
import shlex
import time

import paramiko
from service_group_return_home import run
from service_ssh_broker_lifetime import connect


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--peer', default='wos-3')
    parser.add_argument('--owner', default='wos-0')
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    helper = '/tmp/goal19-group-foreign-' + str(time.time_ns())
    (args.output / 'session.json').write_text(json.dumps({'helper': helper}) + '\n')
    records = []
    with contextlib.closing(paramiko.Agent()) as agent:
        with connect(args, agent.get_keys()[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.put(args.binary, helper)
            sftp.chmod(helper, 0o755)
            for executor in ('main', 'worker'):
                directory = helper + '-' + executor
                sftp.mkdir(directory, 0o777)
                sftp.chmod(directory, 0o777)
                output_dir = args.output / executor
                output_dir.mkdir()
                command = shlex.join(['/usr/bin/locally', helper, '100', '/wki/' + args.owner + directory,
                                      '/wki/' + args.owner + helper, args.peer, args.owner, executor, '--foreign-home'])
                record = {'executor': executor, 'directory': directory, 'command': command, 'started_ns': time.time_ns()}
                records.append(record)
                (args.output / 'results.json').write_text(json.dumps(records, indent=2) + '\n')
                status, out, err = run(transport, command, output_dir / 'stdout.txt')
                (output_dir / 'stdout.txt').write_text(out)
                (output_dir / 'stderr.txt').write_text(err)
                record.update(status=status, ended_ns=time.time_ns())
                print(out, end='', flush=True)
                for leaf in sftp.listdir(directory):
                    if not re.fullmatch('[12]-(ready|observed|advance|failure)', leaf):
                        raise RuntimeError('unexpected marker: ' + leaf)
                    with sftp.open(directory + '/' + leaf, 'rb') as stream:
                        (output_dir / (leaf + '.bin')).write_bytes(stream.read(4096))
                (args.output / 'results.json').write_text(json.dumps(records, indent=2) + '\n')
                match = re.search(r'^PASS foreign source=(\d+) parent=(\d+) branch=(\d+) receiver=(\d+) group=(\d+) query=0 errno=0 signal=0 errno=0 observed=1 identity=1$', out, re.M)
                complete = re.search(r'^PASS complete source=(\d+) status=5888 normal=1 reaped=1$', out, re.M)
                if status or err or not match or not complete or complete[1] != match[1] or match[4] != match[5]:
                    raise RuntimeError('foreign-home behavior requires review: ' + directory)
                record['retirement'] = []
                for role, pid, host in [('source', match[1], args.owner), ('parent', match[2], args.peer),
                                        ('branch', match[3], args.peer), ('receiver', match[4], args.owner)]:
                    if host == args.owner:
                        body = 'test -d /proc/$$ || exit 3; test ! -d /proc/' + pid + ' || exit 4'
                        command = shlex.join(['/usr/bin/locally', '/usr/bin/sh', '-c', body])
                    else:
                        command = shlex.join(['/usr/bin/locally', helper, '--peer-retired', host, pid])
                    status, output, error = run(transport, command)
                    record['retirement'].append({'role': role, 'pid': pid, 'host': host, 'status': status,
                                                 'stdout': output, 'stderr': error})
                    (args.output / 'results.json').write_text(json.dumps(records, indent=2) + '\n')
                    if status or output or error:
                        raise RuntimeError('foreign-home retirement requires review')
                for leaf in sftp.listdir(directory):
                    sftp.remove(directory + '/' + leaf)
                sftp.rmdir(directory)
                record['cleaned'] = True
                (args.output / 'results.json').write_text(json.dumps(records, indent=2) + '\n')
            sftp.remove(helper)
    print('Both foreign-home cases pass; all source/receiver processes absent and fixture files removed.', flush=True)


if __name__ == '__main__':
    main()
