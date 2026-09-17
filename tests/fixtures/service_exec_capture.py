#!/usr/bin/env python3
"""Exercise fallback capture with concurrent and inherited writers in WOS."""
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
    parser.add_argument('--peer', default='wos-3')
    parser.add_argument('--owner', default='wos-0')
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    token = 'goal19-capture-' + str(time.time_ns())
    helper = '/tmp/' + token
    directory = f'/wki/{args.peer}/tmp/' + token
    (args.output / 'session.json').write_text(json.dumps({'helper': helper, 'directory': directory}, indent=2) + '\n')
    with contextlib.closing(paramiko.Agent()) as agent:
        with connect(args, agent.get_keys()[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.put(args.binary, helper)
            sftp.chmod(helper, 0o755)
            sftp.mkdir(directory, 0o700)
            reaped = False
            try:
                argv = ['/usr/bin/locally', '/usr/bin/on', args.peer, f'/wki/{args.owner}' + helper, directory]
                command = shlex.join(argv) + ' </dev/null >/dev/null 2>&1'
                with transport.open_session(timeout=10) as channel:
                    channel.exec_command(command)
                    channel.shutdown_write()
                    deadline = time.monotonic() + 60
                    while not (channel.exit_status_ready() and channel.eof_received):
                        if channel.recv_ready() or channel.recv_stderr_ready():
                            raise RuntimeError('redirected capture produced unexpected SSH output')
                        if time.monotonic() >= deadline:
                            raise RuntimeError('remote root completion deadline')
                        time.sleep(0.01)
                    if channel.recv_ready() or channel.recv_stderr_ready():
                        raise RuntimeError("redirected capture produced unexpected SSH output")
                    status = channel.recv_exit_status()
                (args.output / 'root.json').write_text(json.dumps({'command': command, 'status': status}, indent=2) + '\n')
                if status != 0:
                    raise RuntimeError(f'concurrent root writers failed: {status}')
                with sftp.open(directory + '/gate', 'w') as gate:
                    gate.write('x')
                deadline = time.monotonic() + 15
                report = b''
                while time.monotonic() < deadline:
                    try:
                        with sftp.open(directory + '/report', 'rb') as file:
                            report = file.read(129)
                        if report.endswith(b'\n'):
                            break
                    except OSError as error:
                        if error.errno != errno.ENOENT:
                            raise
                    time.sleep(0.05)
                (args.output / 'report.txt').write_bytes(report)
                match = re.fullmatch(rb'PASS pid=(\d+) late_bytes=22\n', report)
                if match is None:
                    raise RuntimeError(f'inherited writer did not finish: {report!r}')
                pid = match.group(1).decode()
                deadline = time.monotonic() + 15
                while True:
                    # Remote procfs metadata is not exported. Inspect the PID
                    # using native lstat on the peer that owns the process.
                    check = ['/usr/bin/on', args.peer, f'/wki/{args.owner}' + helper, '--reaped', pid]
                    status, output, error = run(transport, shlex.join(check))
                    if output or error or status not in (0, 1):
                        raise RuntimeError(f'peer process check failed: {status}, {output!r}, {error!r}')
                    if status == 0:
                        break
                    if time.monotonic() >= deadline:
                        raise RuntimeError('inherited writer was not reaped')
                    time.sleep(0.05)
                reaped = True
                print('PASS concurrent fallback writers, remote root completion, gated late writes, final close and reaping', flush=True)
            finally:
                # Release a waiting child even if an earlier assertion failed.
                with sftp.open(directory + '/gate', 'w') as gate:
                    gate.write('x')
                if reaped:
                    for name in ('report', 'gate'):
                        sftp.remove(directory + '/' + name)
                    sftp.rmdir(directory)
                    sftp.remove(helper)
                    print('fixture cleanup completed', flush=True)
                else:
                    # Keep the release gate until the child observes it. The
                    # recorded paths permit diagnosis/cleanup of this exact run.
                    print('fixture needs follow-up cleanup:', directory, helper, flush=True)


if __name__ == '__main__':
    main()
