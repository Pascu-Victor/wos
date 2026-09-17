#!/usr/bin/env python3
"""Qualify owner-held executable leases when a disposable consumer disappears."""
import argparse
import contextlib
import json
from pathlib import Path
import shlex
import time

import paramiko

from service_ssh_broker_lifetime import connect
from service_exec_cancellation import peer_identity, row, run
from service_exec_peer_restart import control_vm, rows


def capture(transport, output, label, peer, node=None):
    diag = run(transport, ['/usr/bin/locally', '/usr/bin/cat', '/proc/wki/netdiag'])
    peers = run(transport, ['/usr/bin/locally', '/usr/bin/cat', '/proc/wki/peers'])
    (output / (label + '-netdiag.txt')).write_text(diag)
    (output / (label + '-peers.txt')).write_text(peers)
    if not diag.rstrip().endswith('wki_netdiag_end complete=1'):
        raise RuntimeError('incomplete owner diagnostic snapshot')
    if node is None:
        node = int(peer_identity(peers, peer)[1])
    lifecycle = [r for r in rows(diag, 'wki_peer_lifecycle') if int(r['peer'], 16) == node]
    if len(lifecycle) != 1:
        raise RuntimeError('missing exact consumer lifecycle')
    life = lifecycle[0]
    bindings = [r for r in rows(diag, 'wki_server_binding') if int(r['consumer'], 16) == node
                and r['type'] == 'vfs' and r['active'] == '1' and r['retiring'] == '0']
    return {'node': node, 'epoch': int(life['remote_boot_epoch']), 'state': life['state'],
            'local_channel_epoch': int(life['local_channel_epoch']),
            'remote_channel_epoch': int(life['remote_channel_epoch']),
            'bindings': bindings, 'exec': {k: int(v) for k, v in row(diag, 'wki_vfs_exec').items()}}


def await_state(transport, output, label, peer, predicate, node=None):
    deadline = time.monotonic() + 60
    last = None
    while time.monotonic() < deadline:
        try:
            last = capture(transport, output, label, peer, node)
            if predicate(last):
                return last
        except RuntimeError as error:
            last = str(error)
        time.sleep(1)
    raise RuntimeError(f'{label} did not converge: {last}')


def await_ready(sftp, path, peer, channel):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            with sftp.open(path, 'r') as ready:
                value = ready.read(128).decode()
            if value == f'READY {peer}\n':
                return
        except OSError as error:
            if error.errno != 2:
                raise
        if channel.exit_status_ready():
            raise RuntimeError(f'consumer exited before readiness: status={channel.recv_exit_status()}')
        time.sleep(0.2)
    raise RuntimeError('consumer readiness deadline expired')


def fresh_exec(transport, argv, output):
    # A command's exit status is its completion receipt. Keep observing the
    # same channel through slow recovery instead of treating a short read
    # timeout as termination or resubmitting exec.
    with transport.open_session(timeout=10) as channel:
        channel.exec_command(shlex.join(argv))
        channel.shutdown_write()
        stdout, stderr = bytearray(), bytearray()
        started = time.monotonic()
        while True:
            while channel.recv_ready():
                stdout.extend(channel.recv(8192))
            while channel.recv_stderr_ready():
                stderr.extend(channel.recv_stderr(8192))
            if len(stdout) + len(stderr) > 16384:
                raise RuntimeError('fresh exec output exceeded bound')
            elapsed = time.monotonic() - started
            terminal = channel.exit_status_ready()
            result = {'seconds': elapsed, 'terminal': terminal, 'stdout': stdout.decode(), 'stderr': stderr.decode(),
                      'eof_received': channel.eof_received}
            if terminal:
                result['status'] = channel.recv_exit_status()
            output.write_text(json.dumps(result, indent=2) + '\n')
            if terminal:
                if result['status'] or stderr:
                    raise RuntimeError(f'fresh exec failed: {result}')
                return stdout.decode()
            if elapsed >= 90:
                raise RuntimeError(f'fresh exec did not finish on its original channel: {result}')
            time.sleep(0.05)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--peer', default='wos-3')
    parser.add_argument('--owner', default='wos-0')
    parser.add_argument('--node', type=int, default=3)
    parser.add_argument('--link', default='net0')
    parser.add_argument('--case', choices=['tmpfs-restart', 'tmpfs-link', 'xfs-restart', 'xfs-link'])
    parser.add_argument('--descriptor', type=Path, required=True)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    helper = '/tmp/goal19-consumer-loss-' + str(time.time_ns())
    remote_helper = f'/wki/{args.owner}' + helper
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
                for backend, base in [('tmpfs', '/tmp'), ('xfs', '')]:
                    for mode in ('restart', 'link'):
                        label = backend + '-' + mode
                        if args.case and label != args.case:
                            continue
                        image = base + '/' + helper.rsplit('/', 1)[-1] + '-' + label
                        ready = image + '-ready'
                        control = image + '-control'
                        before = await_state(transport, args.output, label + '-before', args.peer,
                                             lambda s: s['epoch'] != 0 and s['state'] == 'CONNECTED')
                        prepared = run(transport, ['/usr/bin/locally', helper, '--consumer-prepare', image])
                        offset = int(prepared.strip().split('offset=', 1)[1])
                        link_down = False
                        try:
                            with transport.open_session(timeout=10) as local, transport.open_session(timeout=10) as remote:
                                local.settimeout(120)
                                local.exec_command(shlex.join(['/usr/bin/locally', helper, control, '--control-checkpoint']))
                                reader = local.makefile('r')
                                if reader.readline().strip() != 'ADMITTED':
                                    raise RuntimeError('unrelated local image did not start')
                                remote.exec_command(shlex.join(['/usr/bin/on', args.peer, remote_helper, '--consumer-start',
                                                               f'/wki/{args.owner}' + image, f'/wki/{args.owner}' + ready, args.peer]))
                                await_ready(sftp, ready, args.peer, remote)
                                admitted = capture(transport, args.output, label + '-admitted', args.peer)
                                extra = admitted['exec']['owner_views'] - before['exec']['owner_views']
                                if not 1 <= extra <= 3:
                                    raise RuntimeError(f'expected retained remote executable/interpreter views, got {extra}')
                                resources = {r['resource'] for r in admitted['bindings']}
                                if not resources:
                                    raise RuntimeError('missing owner bindings for admitted consumer')
                                proofs = []
                                for path in (image, control):
                                    proofs.append(run(transport, ['/usr/bin/locally', helper, '--consumer-write', path, str(offset), 'blocked']))
                                (args.output / (label + '-proofs.json')).write_text(json.dumps(proofs, indent=2) + '\n')
                                run(transport, ['/usr/bin/locally', '/usr/bin/sync'])
                                if mode == 'restart':
                                    receipt = control_vm(args.descriptor, args.node, args.peer, 'system_reset')
                                else:
                                    receipt = control_vm(args.descriptor, args.node, args.peer, 'set_link', {'name': args.link, 'up': False})
                                    link_down = True
                                (args.output / (label + '-control.json')).write_text(json.dumps(receipt, indent=2) + '\n')
                                retired = await_state(transport, args.output, label + '-retired', args.peer,
                                                      lambda s: s['exec']['owner_views'] == before['exec']['owner_views'] and
                                                      (s['epoch'] != before['epoch'] if mode == 'restart' else
                                                       s['epoch'] == before['epoch'] and s['state'] != 'CONNECTED'), before['node'])
                                proofs.append(run(transport, ['/usr/bin/locally', helper, '--consumer-write', image, str(offset), 'released']))
                                proofs.append(run(transport, ['/usr/bin/locally', helper, '--consumer-write', control, str(offset), 'blocked']))
                                (args.output / (label + '-proofs.json')).write_text(json.dumps(proofs, indent=2) + '\n')
                                if mode == 'link':
                                    control_vm(args.descriptor, args.node, args.peer, 'set_link', {'name': args.link, 'up': True})
                                    link_down = False
                                healed = await_state(transport, args.output, label + '-healed', args.peer,
                                                     lambda s: s['state'] == 'CONNECTED' and
                                                     (s['epoch'] != before['epoch'] if mode == 'restart' else s['epoch'] == before['epoch']) and
                                                     resources <= {r['resource'] for r in s['bindings']},
                                                     before['node'])
                                # Channel generation belongs to a reusable pool slot;
                                # (channel number, generation) alone is not an identity
                                # across an authenticated session/epoch transition.
                                old_epochs = (admitted['local_channel_epoch'], admitted['remote_channel_epoch'])
                                new_epochs = (healed['local_channel_epoch'], healed['remote_channel_epoch'])
                                if old_epochs == new_epochs:
                                    raise RuntimeError('consumer recovery did not retire the old channel session')
                                deadline = time.monotonic() + 30
                                while not remote.exit_status_ready() and time.monotonic() < deadline:
                                    time.sleep(0.2)
                                if not remote.exit_status_ready():
                                    raise RuntimeError('old consumer command did not terminate')
                                old_status = remote.recv_exit_status()
                                (args.output / (label + '-old-status.txt')).write_text(str(old_status) + '\n')
                                if old_status == 0:
                                    raise RuntimeError('interrupted hold command unexpectedly succeeded')
                                remote.close()
                                fresh = fresh_exec(transport, ['/usr/bin/on', args.peer, remote_helper, '--consumer-fresh',
                                                               f'/wki/{args.owner}' + image], args.output / (label + '-fresh.json'))
                                if fresh != 'released-image':
                                    raise RuntimeError('fresh consumer did not read replacement bytes: ' + repr(fresh))
                                local.sendall(b'G')
                                exited = reader.readline().strip()
                                if exited != 'EXIT ok=1 status=0':
                                    raise RuntimeError('unrelated local image failed: ' + exited)
                                local.sendall(b'H')
                                final = reader.readline().strip()
                                if final != 'PASS cancellation released=1 cleanup=1' or local.recv_exit_status() != 0:
                                    raise RuntimeError('local control cleanup failed: ' + final)
                                after = await_state(transport, args.output, label + '-after', args.peer,
                                                    lambda s: s['exec']['owner_views'] == before['exec']['owner_views'])
                                result = {'case': label, 'before': before, 'admitted': admitted, 'retired': retired,
                                          'healed': healed, 'after': after, 'old_status': old_status, 'proofs': proofs, 'pass': True}
                                results.append(result)
                                (args.output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
                                print(f'PASS {label}: {extra} owner views retired, local lease preserved, fresh consumer read', flush=True)
                        finally:
                            if link_down:
                                control_vm(args.descriptor, args.node, args.peer, 'set_link', {'name': args.link, 'up': True})
                            for path in (image, ready, control):
                                try:
                                    sftp.remove(path)
                                except OSError as error:
                                    if error.errno != 2:
                                        raise
            finally:
                sftp.remove(helper)
                print('fixture cleanup completed', flush=True)


if __name__ == '__main__':
    main()
