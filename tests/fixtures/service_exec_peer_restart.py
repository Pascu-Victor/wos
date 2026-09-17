#!/usr/bin/env python3
"""Check retained executable views across a disposable WOS owner VM restart."""
import argparse
import contextlib
import json
from pathlib import Path
import shlex
import socket
import time

import paramiko

from service_ssh_broker_lifetime import connect
from service_exec_cancellation import count, peer_identity, row, run


def rows(text, prefix):
    return [dict(token.split('=', 1) for token in line.split()[1:])
            for line in text.splitlines() if line.startswith(prefix + ' ')]


def snapshot(transport, output, label, peer):
    diag = run(transport, ['/usr/bin/locally', '/usr/bin/cat', '/proc/wki/netdiag'])
    peers = run(transport, ['/usr/bin/locally', '/usr/bin/cat', '/proc/wki/peers'])
    (output / (label + '-netdiag.txt')).write_text(diag)
    (output / (label + '-peers.txt')).write_text(peers)
    if not diag.rstrip().endswith('wki_netdiag_end complete=1'):
        raise RuntimeError('incomplete diagnostic snapshot')
    identity = peer_identity(peers, peer)
    node = int(identity[1])
    lifecycle = [r for r in rows(diag, 'wki_peer_lifecycle') if int(r['peer'], 16) == node]
    if len(lifecycle) != 1:
        raise RuntimeError('missing exact peer lifecycle')
    epoch = int(lifecycle[0]['remote_boot_epoch'])
    active = [r for r in rows(diag, 'wki_vfs_proxy') if int(r['owner'], 16) == node
              and int(r['owner_boot']) == epoch and r['active'] == '1' and r['lanes_ready'] == '1']
    return {'node': node, 'epoch': epoch, 'active_proxies': active,
            'exec': {k: int(v) for k, v in row(diag, 'wki_vfs_exec').items()}}


def control_vm(descriptor_path, node, peer, operation, arguments=None):
    arguments = arguments or {}
    if operation not in ('system_reset', 'set_link'):
        raise RuntimeError('unsupported disposable VM operation')
    if operation == 'system_reset' and arguments:
        raise RuntimeError('reset takes no arguments')
    if operation == 'set_link' and (set(arguments) != {'name', 'up'} or not isinstance(arguments['up'], bool)):
        raise RuntimeError('link control requires an exact NIC name and boolean state')
    descriptor_path = descriptor_path.resolve()
    descriptor = json.loads(descriptor_path.read_text())
    targets = [t for t in descriptor['targets'] if t['nodeId'] == node and t['hostname'] == peer]
    if len(targets) != 1 or descriptor['kind'] != 'cluster':
        raise RuntimeError('restart requires one exact disposable cluster target')
    target = targets[0]
    proc = Path(f"/proc/{target['pid']}")
    argv = (proc / 'cmdline').read_bytes().split(b'\0')
    expected = str(descriptor_path.parent / f'disk-vm{node}.qcow2')
    if not any(b'qemu-system-x86_64' in arg for arg in argv) or not any(
            arg.decode().startswith('file=' + expected + ',') for arg in argv):
        raise RuntimeError('QEMU command line does not match disposable overlay')
    ticks = (proc / 'stat').read_text().rsplit(')', 1)[1].split()[19]
    if ticks != target['processStartTicks']:
        raise RuntimeError('QEMU process identity changed')
    if operation == 'set_link' and not any(
            argv[i] == b'-netdev' and ('id=' + arguments['name']) in argv[i + 1].decode().split(',')
            for i in range(len(argv) - 1)):
        raise RuntimeError('link is not a configured NIC of this disposable VM')
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(10)
        sock.connect(target['qmpSocket'])
        with sock.makefile('rwb', buffering=0) as stream:
            json.loads(stream.readline())
            for command, params in [('qmp_capabilities', {}), (operation, arguments)]:
                stream.write((json.dumps({'execute': command, 'arguments': params}) + '\r\n').encode())
                while True:
                    response = json.loads(stream.readline())
                    if 'error' in response:
                        raise RuntimeError(response)
                    if 'return' in response:
                        break
    return {'node': node, 'pid': target['pid'], 'start_ticks': ticks, 'overlay': expected,
            'operation': operation, 'arguments': arguments}


def restart_owner(descriptor_path, node, peer):
    return control_vm(descriptor_path, node, peer, 'system_reset')


def wait_snapshot(transport, output, label, peer, predicate):
    deadline = time.monotonic() + 90
    last = None
    while time.monotonic() < deadline:
        try:
            last = snapshot(transport, output, label, peer)
            if predicate(last):
                return last
        except RuntimeError as error:
            last = str(error)
        time.sleep(1)
    raise RuntimeError(f'{label} did not converge: {last}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--peer', default='wos-3')
    parser.add_argument('--node', type=int, default=3)
    parser.add_argument('--descriptor', type=Path, required=True)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    helper = '/tmp/goal19-peer-restart-' + str(time.time_ns())
    results = []
    with contextlib.closing(paramiko.Agent()) as agent, contextlib.ExitStack() as stack:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError('authorized SSH agent key required')
        deadline = time.monotonic() + 120
        while True:
            try:
                transport = stack.enter_context(connect(args, keys[0]))
                break
            except (OSError, paramiko.SSHException):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1)
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.put(args.binary, helper)
            sftp.chmod(helper, 0o755)
            try:
                for backend, base in [('tmpfs', f'/wki/{args.peer}/tmp'), ('xfs', f'/wki/{args.peer}')]:
                    path = base + '/' + helper.rsplit('/', 1)[-1]
                    before = wait_snapshot(transport, args.output, backend + '-before', args.peer,
                                           lambda s: s['epoch'] != 0 and any(r['mount'] == base for r in s['active_proxies']))
                    with transport.open_session(timeout=10) as channel:
                        channel.settimeout(120)
                        channel.exec_command(shlex.join(['/usr/bin/locally', helper, path, '--peer-restart']))
                        reader = channel.makefile('r')
                        with (args.output / (backend + '-fixture.txt')).open('w') as log:
                            def line():
                                value = reader.readline()
                                log.write(value)
                                log.flush()
                                return value.strip()
                            if line() != 'ADMITTED':
                                raise RuntimeError('child did not admit remote image')
                            admitted = snapshot(transport, args.output, backend + '-admitted', args.peer)
                            if count(admitted['exec']) != count(before['exec']) + 1:
                                raise RuntimeError('expected exactly one retained executable view')
                            run(transport, ['/usr/bin/on', args.peer, '/usr/bin/sync'])
                            reset = restart_owner(args.descriptor, args.node, args.peer)
                            (args.output / (backend + '-reset.json')).write_text(json.dumps(reset, indent=2) + '\n')
                            changed = wait_snapshot(transport, args.output, backend + '-restarted', args.peer,
                                                    lambda s: s['epoch'] != 0 and s['epoch'] != before['epoch'])
                            if count(changed['exec']) != count(admitted['exec']):
                                raise RuntimeError('live image lost its owned cancellation record before close')
                            channel.sendall(b'G')
                            retired = line()
                            if not retired.startswith('RETIRED ok=1 '):
                                raise RuntimeError('stale binding/image contract failed: ' + retired)
                            after = wait_snapshot(transport, args.output, backend + '-retired', args.peer,
                                                  lambda s: count(s['exec']) == count(before['exec']) and
                                                  s['exec']['retirement_handoffs'] > before['exec']['retirement_handoffs'] and
                                                  any(r['mount'] == base for r in s['active_proxies']))
                            channel.sendall(b'H')
                            final = line()
                            status = channel.recv_exit_status()
                            error = channel.recv_stderr(8192).decode()
                            if final != 'PASS peer restart stale_refused=1 old_image=1 fresh=1 cleanup=1' or status or error:
                                raise RuntimeError(f'fresh image/cleanup failed: {final!r} status={status} stderr={error!r}')
                            result = {'backend': backend, 'before': before, 'admitted': admitted,
                                      'restarted': changed, 'retired': after, 'native': retired, 'pass': True}
                            results.append(result)
                            (args.output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
                            print(f"PASS {backend} owner boot {before['epoch']} -> {changed['epoch']}; stale refused, view retired, fresh exec", flush=True)
                    try:
                        sftp.remove(path)
                    except OSError as error:
                        if error.errno != 2:
                            raise
            finally:
                sftp.remove(helper)
                print('uploaded helper cleanup completed', flush=True)


if __name__ == '__main__':
    main()
