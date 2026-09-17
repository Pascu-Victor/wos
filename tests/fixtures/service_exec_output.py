#!/usr/bin/env python3
"""Compare exact local/remote stdout bytes through the normal SSH command path."""
import argparse
import contextlib
import hashlib
import json
from pathlib import Path
import shlex
import time
import paramiko
from service_ssh_broker_lifetime import connect


def collect(transport, argv, path, lifecycle='immediate', pty=False):
    with transport.open_session(timeout=10) as channel:
        if pty:
            channel.get_pty(term='dumb', width=80, height=24)
        channel.exec_command(shlex.join(argv))
        released = lifecycle == 'immediate'
        if released and not pty:
            channel.shutdown_write()
        stdout, stderr = bytearray(), bytearray()
        deadline = time.monotonic() + 90
        while True:
            while channel.recv_ready():
                stdout.extend(channel.recv(65536))
            while channel.recv_stderr_ready():
                stderr.extend(channel.recv_stderr(8192))
            if not released and ((lifecycle == 'inherited' and channel.exit_status_ready()) or
                                 (lifecycle == 'close' and channel.eof_received)):
                if lifecycle == 'close':
                    channel.sendall(b'x')
                channel.shutdown_write()
                released = True
            if len(stdout) > 2 * 1024 * 1024 or len(stderr) > 8192:
                raise RuntimeError('output exceeded fixture bound')
            if channel.exit_status_ready() and channel.eof_received:
                # Both events can arrive between the drain and this check.
                if channel.recv_ready() or channel.recv_stderr_ready():
                    continue
                result = {'status': channel.recv_exit_status(), 'bytes': len(stdout), 'released': released,
                          'stderr': stderr.decode(), 'sha256': hashlib.sha256(stdout).hexdigest()}
                path.with_suffix('.bin').write_bytes(stdout)
                path.with_suffix('.json').write_text(json.dumps(result, indent=2) + '\n')
                return result, bytes(stdout)
            if time.monotonic() >= deadline:
                raise RuntimeError('original output command did not finish')
            time.sleep(0.01)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--peer', default='wos-3')
    parser.add_argument('--owner', default='wos-0')
    parser.add_argument('--binary', required=True)
    parser.add_argument('--hold-ms', type=int, default=0)
    parser.add_argument('--pty', action='store_true', help='Check raw PTY output and its merged stderr receipt')
    parser.add_argument('--pty-receipt', choices=('stderr', 'stdout'), default='stderr',
                        help='PTY diagnostic: compare aliased stderr with a receipt on the stdout stream')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if not 0 <= args.hold_ms <= 5000:
        parser.error('--hold-ms must be between 0 and 5000')
    if not args.pty and args.pty_receipt != 'stderr':
        parser.error('--pty-receipt requires --pty')
    args.output.mkdir(parents=True, exist_ok=False)
    helper = '/tmp/goal19-output-' + str(time.time_ns())
    (args.output / 'session.json').write_text(json.dumps({
        'helper': helper, 'pty': args.pty, 'pty_receipt': args.pty_receipt, 'hold_ms': args.hold_ms,
    }, indent=2) + '\n')
    pattern = bytes((i * 73 + 19) & 255 for i in range(256))
    failures = []
    with contextlib.closing(paramiko.Agent()) as agent:
        with connect(args, agent.get_keys()[0]) as t, paramiko.SFTPClient.from_transport(t) as sftp:
            try:
                sftp.put(args.binary, helper)
                sftp.chmod(helper, 0o755)
                for size in (4096, 131072, 1048576):
                    for mode in ('local', 'remote'):
                        argv = ['/usr/bin/locally', helper] if mode == 'local' else [
                            '/usr/bin/on', args.peer, f'/wki/{args.owner}' + helper]
                        command = argv + [str(size), str(args.hold_ms)]
                        if args.pty:
                            command.append('raw-terminal-single' if args.pty_receipt == 'stdout' else 'raw-terminal')
                        result, data = collect(t, command, args.output / f'{mode}-{size}', pty=args.pty)
                        expected = pattern * (size // len(pattern))
                        receipt = f'EMITTED bytes={size}\n'
                        if args.pty:
                            expected += receipt.encode()
                        passed = result['status'] == 0 and data == expected and result['stderr'] == ('' if args.pty else receipt)
                        print(('PASS' if passed else 'FAIL'), mode, size, result, flush=True)
                        if not passed:
                            failures.append({'case': f'{mode}-{size}', **result})
                if args.hold_ms == 0 and not args.pty:
                    for lifecycle in ('inherited', 'close'):
                        for mode in ('local', 'remote'):
                            argv = ['/usr/bin/locally', helper] if mode == 'local' else [
                                '/usr/bin/on', args.peer, f'/wki/{args.owner}' + helper]
                            size = 131072
                            case = f'{mode}-{lifecycle}'
                            result, data = collect(t, argv + [str(size), '0', lifecycle], args.output / case, lifecycle)
                            passed = (result['status'] == 0 and result['released'] and data == pattern * (size // len(pattern))
                                      and result['stderr'] == f'EMITTED bytes={size}\n')
                            print(('PASS' if passed else 'FAIL'), case, result, flush=True)
                            if not passed:
                                failures.append({'case': case, **result})
            finally:
                sftp.remove(helper)
                print('fixture cleanup completed', flush=True)
    (args.output / 'failures.json').write_text(json.dumps(failures, indent=2) + '\n')
    if failures:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
