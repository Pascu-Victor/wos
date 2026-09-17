#!/usr/bin/env python3
"""Verify service sessions and optional HTTPD lifecycle in a disposable WOS VM."""
import argparse
import contextlib
import re
import time

import paramiko

from service_ssh_broker_lifetime import connect, run

ROW = re.compile(r'^(\S+)\s+(\S+)\s+(\S+)\s+pid=(-?\d+) pgid=(-?\d+) gen=(\d+) ')


def status(transport):
    code, output = run(transport, '/usr/bin/servicectl status')
    assert code == 0, output
    rows = {}
    for line in output.splitlines():
        match = ROW.match(line)
        if match:
            name, state, ready, pid, pgid, generation = match.groups()
            rows[name] = (state, ready, int(pid), int(pgid), int(generation))
    assert rows, output
    return rows


def verify(transport, name, row):
    state, ready, pid, pgid, generation = row
    assert state == 'running' and ready == 'ready' and pid > 1 and pgid == pid, (name, row)
    code, output = run(transport, f'cat /proc/{pid}/status')
    assert code == 0, output
    fields = dict(line.split(':', 1) for line in output.splitlines() if ':' in line)
    assert int(fields['Pid']) == int(fields['PGID']) == int(fields['SID']) == pid, fields
    assert int(fields['PPid']) == 1, fields
    print(f'PASS {name} pid=pgid=sid={pid} generation={generation} uid={fields["Uid"].strip()}', flush=True)


def wait_state(transport, name, expected, generation=None):
    deadline = time.monotonic() + 20
    while True:
        row = status(transport)[name]
        if row[0] == expected and (generation is None or row[4] > generation):
            if expected != 'running' or row[1] == 'ready':
                return row
        assert time.monotonic() < deadline, (name, expected, row)
        time.sleep(0.1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--restart-httpd', action='store_true')
    args = parser.parse_args()
    with contextlib.closing(paramiko.Agent()) as agent, connect(args, agent.get_keys()[0]) as transport:
        initial = status(transport)
        for name in ('journald', 'netd', 'httpd', 'dropbear'):
            verify(transport, name, initial[name])
        for name, row in initial.items():
            if row[2] > 1 and name not in ('journald', 'netd', 'httpd', 'dropbear'):
                verify(transport, name, row)
        if args.restart_httpd:
            code, before = run(transport, 'ls /proc/1/fd')
            assert code == 0, before
            previous = initial['httpd']
            for attempt in range(4):
                code, output = run(transport, '/usr/bin/servicectl restart httpd')
                assert code == 0, output
                current = wait_state(transport, 'httpd', 'running', previous[4])
                assert current[2] != previous[2], (previous, current)
                verify(transport, 'httpd', current)
                previous = current
            assert run(transport, '/usr/bin/servicectl stop httpd')[0] == 0
            stopped = wait_state(transport, 'httpd', 'exited')
            assert stopped[2:4] == (-1, -1), stopped
            assert run(transport, '/usr/bin/servicectl start httpd')[0] == 0
            verify(transport, 'httpd', wait_state(transport, 'httpd', 'running', previous[4]))
            code, after = run(transport, 'ls /proc/1/fd')
            assert code == 0 and before.split() == after.split(), (before, after)
            print('PASS four HTTPD replacements, stop/start quiescence and unchanged PID1 descriptor set', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
