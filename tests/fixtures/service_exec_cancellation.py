#!/usr/bin/env python3
"""Exercise remote executable cleanup across bounded WKI delivery interruptions."""
import argparse
import contextlib
import json
from pathlib import Path
import shlex
import time

import paramiko
from service_ssh_broker_lifetime import connect


def run(transport, argv):
    with transport.open_session(timeout=10) as channel:
        channel.settimeout(15)
        channel.exec_command(shlex.join(argv))
        output = bytearray()
        while chunk := channel.recv(8192):
            output.extend(chunk)
            if len(output) > 1048576:
                raise RuntimeError('diagnostic output exceeded bound')
        error = channel.recv_stderr(8192).decode()
        status = channel.recv_exit_status()
        if status or error:
            raise RuntimeError(f'{argv}: status={status} stderr={error!r}')
        return output.decode()


def row(text, prefix):
    rows = [line for line in text.splitlines() if line.startswith(prefix + ' ')]
    if len(rows) != 1:
        raise RuntimeError(f'expected one {prefix} row')
    return dict(token.split('=', 1) for token in rows[0].split()[1:])


def snapshot(transport, output, label):
    diag = run(transport, ['/usr/bin/cat', '/proc/wki/netdiag'])
    peers = run(transport, ['/usr/bin/cat', '/proc/wki/peers'])
    (output / (label + '-netdiag.txt')).write_text(diag)
    (output / (label + '-peers.txt')).write_text(peers)
    if not diag.rstrip().endswith('wki_netdiag_end complete=1'):
        raise RuntimeError('incomplete kernel diagnostic snapshot')
    return {key: int(value) for key, value in row(diag, 'wki_vfs_exec').items()}, peers


def peer_identity(peers, hostname):
    rows = [line.split() for line in peers.splitlines() if line.split() and line.split()[0] == hostname]
    if len(rows) != 1 or len(rows[0]) != 7 or rows[0][2] != '1':
        raise RuntimeError('expected connected peer ' + hostname)
    return rows[0][:3]


def count(diag):
    return sum(diag[key] for key in ('owned', 'pending', 'retrying'))


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
    results = []
    helper = '/tmp/goal19-cancel-' + str(time.time_ns())
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError('authorized SSH agent key required')
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.put(args.binary, helper)
            sftp.chmod(helper, 0o755)
            try:
                for backend, base in [('tmpfs', f'/wki/{args.peer}/tmp'), ('xfs', f'/wki/{args.peer}')]:
                    for direction, msg, selector in [('tx', '0x43', 'dst_host'), ('rx', '0x44', 'src_host')]:
                        name = backend + '-' + direction
                        path = base + '/' + helper.rsplit('/', 1)[-1] + '-' + direction
                        run(transport, ['/usr/bin/wkictl', 'chaos', 'clear'])
                        before, peers = snapshot(transport, output, name + '-before')
                        identity = peer_identity(peers, args.peer)
                        with transport.open_session(timeout=10) as channel:
                            channel.settimeout(90)
                            channel.exec_command(shlex.join([helper, path, '--cancel-checkpoint']))
                            reader = channel.makefile('r')
                            try:
                                first = reader.readline()
                                (output / (name + '-fixture.txt')).write_text(first)
                                if first.strip() != 'ADMITTED':
                                    raise RuntimeError('remote child did not reach checkpoint: ' + first)
                                admitted, _ = snapshot(transport, output, name + '-admitted')
                                if count(admitted) != count(before) + 1:
                                    raise RuntimeError('checkpoint must own exactly one new remote image view')
                                run(transport, ['/usr/bin/wkictl', 'chaos', 'enable', 'seed=1909'])
                                # Held retransmissions coalesce into fixed injector storage.
                                # This preserves complete evidence through the real 60s RPC timeout.
                                run(transport, ['/usr/bin/wkictl', 'chaos', 'rule', 'id=19', 'dir=' + direction,
                                                'action=partition', 'type=' + msg, 'op=0x041d', selector + '=' + args.peer])
                                started = time.monotonic()
                                channel.sendall(b'G')
                                exited = reader.readline()
                                elapsed = time.monotonic() - started
                                with (output / (name + '-fixture.txt')).open('a') as log:
                                    log.write(exited)
                                if exited.strip() != 'EXIT ok=1 status=0' or elapsed < 55 or elapsed > 85:
                                    raise RuntimeError(f'cleanup did not traverse the expected timeout: {exited!r} elapsed={elapsed}')
                                held, peers = snapshot(transport, output, name + '-held')
                                if peer_identity(peers, args.peer) != identity:
                                    raise RuntimeError('peer changed or disconnected during cancellation interruption')
                                if count(held) != count(before) + 1 or held['pending'] + held['retrying'] < 1:
                                    raise RuntimeError('failed close did not retain its cancellation record')
                                if held['failed_attempts'] <= before['failed_attempts']:
                                    raise RuntimeError('missing failed cleanup attempt evidence')
                                chaos = run(transport, ['/usr/bin/cat', '/proc/wki/chaos'])
                                (output / (name + '-held-chaos.txt')).write_text(chaos)
                                state, rule = row(chaos, 'wki_chaos'), row(chaos, 'wki_chaos_rule')
                                if not chaos.rstrip().endswith('wki_chaos_end complete=1') or any(state[key] != '0' for key in
                                        ('invalid', 'queue_overflow', 'trace_overflow', 'stream_overflow')):
                                    raise RuntimeError('incomplete injector evidence')
                                if int(rule['applied']) < 1 or int(state['queued']) < 1:
                                    raise RuntimeError('target cancellation frames were not held')
                                run(transport, ['/usr/bin/wkictl', 'chaos', 'heal', 'id=19'])
                                deadline = time.monotonic() + 20
                                sample = 0
                                while True:
                                    after, peers = snapshot(transport, output, name + f'-healed-{sample}')
                                    sample += 1
                                    if peer_identity(peers, args.peer) != identity:
                                        raise RuntimeError('peer changed during cleanup recovery')
                                    if count(after) == count(before) and after['cancelled'] > before['cancelled']:
                                        break
                                    if time.monotonic() >= deadline:
                                        raise RuntimeError('cancellation ownership did not converge after heal')
                                    time.sleep(0.1)
                                if after['retirement_handoffs'] != before['retirement_handoffs'] or after['attempts'] < before['attempts'] + 2:
                                    raise RuntimeError('cleanup must complete by retry, not peer retirement')
                                final_chaos = run(transport, ['/usr/bin/cat', '/proc/wki/chaos'])
                                (output / (name + '-healed-chaos.txt')).write_text(final_chaos)
                                if row(final_chaos, 'wki_chaos')['queued'] != '0':
                                    raise RuntimeError('injector still owns held frames')
                                channel.sendall(b'H')
                                final = reader.readline()
                                with (output / (name + '-fixture.txt')).open('a') as log:
                                    log.write(final)
                                status = channel.recv_exit_status()
                                error = channel.recv_stderr(8192).decode()
                                if final.strip() != 'PASS cancellation released=1 cleanup=1' or status or error:
                                    raise RuntimeError(f'owner write/fixture cleanup failed: {final!r} {status} {error!r}')
                                result = dict(case=name, passed=True, timeout_seconds=elapsed, before=before, held=held, after=after)
                                results.append(result)
                                (output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
                                print('PASS', name, 'timeout', round(elapsed, 3), 'record converged; owner writable; cleanup complete', flush=True)
                            finally:
                                run(transport, ['/usr/bin/wkictl', 'chaos', 'clear'])
                                try:
                                    channel.sendall(b'H')
                                    channel.shutdown_write()
                                except (OSError, EOFError):
                                    pass
                                reader.close()
                                try:
                                    sftp.remove(path)
                                except OSError as error:
                                    if error.errno != 2:
                                        raise
            finally:
                run(transport, ['/usr/bin/wkictl', 'chaos', 'clear'])
                sftp.remove(helper)
                print('fixture cleanup completed', flush=True)


if __name__ == '__main__':
    main()
