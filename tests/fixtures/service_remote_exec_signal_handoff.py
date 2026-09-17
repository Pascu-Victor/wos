#!/usr/bin/env python3
"""Observe owned-child signals before, during and after remote exec publication."""
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


class Conversation:
    def __init__(self, transport, command, logfile):
        self.channel = transport.open_session(timeout=10)
        self.channel.settimeout(20)
        self.channel.exec_command(command)
        self.buffer = bytearray()
        self.logfile = logfile

    def line(self, prefix):
        while b'\n' not in self.buffer:
            chunk = self.channel.recv(1024)
            if not chunk:
                raise RuntimeError('fixture closed before ' + prefix)
            self.buffer.extend(chunk)
            if len(self.buffer) > 8192:
                raise RuntimeError('fixture output bound')
        raw, _, self.buffer = self.buffer.partition(b'\n')
        value = raw.decode().strip()
        with self.logfile.open('a') as log:
            log.write(value + '\n')
        if not value.startswith(prefix):
            raise RuntimeError(f'expected {prefix!r}, received {value!r}')
        return value

    def send(self, value):
        self.channel.sendall(value + '\n')

    def finish(self):
        status = self.channel.recv_exit_status()
        error = self.channel.recv_stderr(4096).decode()
        self.channel.close()
        if error:
            raise RuntimeError(error)
        return status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--peer', default='wos-3')
    parser.add_argument('--owner', default='wos-0')
    parser.add_argument('--binary', required=True)
    parser.add_argument('--group', action='store_true', help='signal only the owned child group; include the post-COMMIT hold')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    helper = '/tmp/goal19-signal-handoff-' + str(time.time_ns())
    records = []
    phases = [('before', 'local'), ('before', 'remote'), ('held', 'remote'), ('after', 'remote')]
    if args.group:
        phases.insert(3, ('committed', 'remote'))
    (args.output / 'session.json').write_text(json.dumps({'helper': helper}, indent=2) + '\n')
    with contextlib.closing(paramiko.Agent()) as agent:
        with connect(args, agent.get_keys()[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.put(args.binary, helper)
            sftp.chmod(helper, 0o755)
            controller = Conversation(transport, shlex.join(['/usr/bin/on', args.peer, '/wki/' + args.owner + helper, '--controller']),
                                      args.output / 'controller.log')
            controller.line('CONTROLLER_READY')
            held = False
            try:
                for phase, placement in phases:
                    for uid in (0, 100):
                        for executor in ('main', 'worker'):
                            index = len(records)
                            directory = helper + '-case-' + str(index)
                            sftp.mkdir(directory, 0o777)
                            sftp.chmod(directory, 0o777)
                            command = shlex.join(['/usr/bin/locally', helper, str(uid), placement, executor,
                                                 '/wki/' + args.owner + helper, directory, args.peer, args.owner] + (['--group'] if args.group else []))
                            record = {'phase': phase, 'placement': placement, 'uid': uid, 'executor': executor,
                                      'directory': directory, 'command': command, 'group': args.group, 'started_ns': time.time_ns()}
                            records.append(record)
                            (args.output / 'results.json').write_text(json.dumps(records, indent=2) + '\n')
                            source = Conversation(transport, command, args.output / f'case-{index}.log')
                            record['ready'] = source.line('SOURCE_READY')
                            if phase in ('held', 'committed'):
                                controller.send('C' if phase == 'committed' else 'H')
                                controller.line('HOLD_ENABLED')
                                held = True
                            if phase == 'before':
                                source.send('Q')
                                record['query'] = source.line('QUERY')
                                source.send('S')
                                record['signal'] = source.line('SIGNAL')
                            source.send('G')
                            source.line('SOURCE_STARTED')
                            if phase in ('held', 'committed'):
                                controller.send('W')
                                record['hold'] = controller.line('WAITER_CONFIRMED')
                                record['witness_ns'] = time.time_ns()
                            if phase == 'after':
                                source.send('P')
                                record['accepted'] = source.line('PAYLOAD_READY ready=1 accepted=1 ')
                            if phase != 'before':
                                source.send('Q')
                                record['query'] = source.line('QUERY')
                                source.send('S')
                                record['signal'] = source.line('SIGNAL')
                            if phase in ('held', 'committed'):
                                controller.send('R')
                                controller.line('HOLD_RELEASED')
                                record['release_ns'] = time.time_ns()
                                held = False
                            source.send('D')
                            record['result'] = source.line('')
                            record['status'] = source.finish()
                            (args.output / 'results.json').write_text(json.dumps(records, indent=2) + '\n')
                            print(phase, placement, uid, executor, record['result'], flush=True)
                            if not record['result'].startswith(('PASS ', 'FAIL ')) or not record['result'].endswith('reaped=1') or 'found=1' not in record['result']:
                                raise RuntimeError('child lifetime requires review: ' + directory)
                            if 'exec_error=0 ' not in record['result'] or 'identity=1 ' not in record['result']:
                                raise RuntimeError('exec did not reach replacement; hold timing is inconclusive: ' + directory)
                            record['admitted'] = (record['signal'] == 'SIGNAL result=0 errno=0' and
                                                  record['query'] == 'QUERY result=0 errno=0')
                            if placement == 'remote':
                                match = re.search(r' receiver=(\d+) ', record['result'])
                                if not match or int(match[1]) <= 0:
                                    raise RuntimeError('receiver identity unavailable')
                                retired, out, err = run(transport, shlex.join(['/usr/bin/on', args.peer, '/usr/bin/sh', '-c',
                                    'test -d /proc/$$ || exit 3; test ! -d /proc/' + match[1] + ' || exit 4']))
                                record['receiver_retired'] = {'status': retired, 'stdout': out, 'stderr': err}
                                if retired != 0 or out or err:
                                    raise RuntimeError('receiver lifetime requires review: ' + directory)
                            for leaf in ('ready', 'inspect', 'result'):
                                sftp.remove(directory + '/' + leaf)
                            sftp.rmdir(directory)
                            record['cleaned'] = True
                            (args.output / 'results.json').write_text(json.dumps(records, indent=2) + '\n')
            finally:
                if held:
                    controller.send('R')
                    controller.line('HOLD_RELEASED')
                controller.send('Q')
                controller.line('CONTROLLER_DONE')
                if controller.finish() != 0:
                    raise RuntimeError('publication controller cleanup failed')
            sftp.remove(helper)
            print('all fixture children reaped; receiver processes absent; helper and control files removed', flush=True)
    raise SystemExit(1 if any(row['status'] or not row['admitted'] or not row['result'].startswith('PASS ') for row in records) else 0)


if __name__ == '__main__':
    main()
