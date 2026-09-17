#!/usr/bin/env python3
"""Pause an owned source VM at a claimed wait; observe receiver fatal unwinding."""
import argparse
import contextlib
import json
from pathlib import Path
import shlex
import socket
import subprocess
import time
from types import SimpleNamespace

import paramiko
from service_ssh_broker_lifetime import connect
from service_wki_identity import run
from service_exec_peer_restart import snapshot, wait_snapshot


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--kernel', required=True)
    parser.add_argument('--live', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--worker-exit', action='store_true', help='Request main-thread exit while a worker owns COMMIT')
    parser.add_argument('--worker-exec', action='store_true', help='Locally exec main while a worker owns COMMIT')
    parser.add_argument('--gdb-socket', type=Path, help='Reuse an existing private listener on the exact receiver VM')
    parser.add_argument('--receiver-reset', action='store_true', help='Reset the exact receiver VM while COMMIT is pending')
    args = parser.parse_args()
    assert sum([args.worker_exit, args.worker_exec, args.receiver_reset]) <= 1
    worker = args.worker_exit or args.worker_exec
    args.output.mkdir(parents=True, exist_ok=False)
    root = args.output.resolve()
    live = json.loads(args.live.read_text())
    targets = {int(x['nodeId']): x for x in live['targets']}
    symbols = subprocess.check_output(['nm', args.kernel], text=True).splitlines()
    symbol = '_ZN3ker3net3wki21wki_child_wait_commitERNS_3mod5sched4task4TaskEm'
    matches = [line.split()[0] for line in symbols if line.endswith(' ' + symbol)]
    assert len(matches) == 1
    commit_address = int(matches[0], 16)
    helper = '/tmp/goal19-wait-termination-' + str(time.time_ns())
    result = {'helper': helper, 'worker_exit': args.worker_exit, 'worker_exec': args.worker_exec,
              'receiver_reset': args.receiver_reset, 'cases': [], 'removed': [], 'markers_removed': [], 'qmp': []}

    def save():
        (root / 'results.json').write_text(json.dumps(result, indent=2) + '\n')

    def qmp(node, command, arguments=None):
        target = targets[node]
        proc = Path('/proc') / str(target['pid'])
        assert (proc / 'stat').read_text().rsplit(')', 1)[1].split()[19] == target['processStartTicks']
        assert target['qmpSocket'] in (proc / 'cmdline').read_bytes().decode()
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(5)
            sock.connect(target['qmpSocket'])
            with sock.makefile('rwb') as stream:
                json.loads(stream.readline())
                response = None
                for name, body in [('qmp_capabilities', None), (command, arguments)]:
                    stream.write((json.dumps({'execute': name, **({'arguments': body} if body else {})}) + '\n').encode())
                    stream.flush()
                    while True:
                        response = json.loads(stream.readline())
                        if 'return' in response or 'error' in response:
                            assert 'error' not in response, response
                            break
        result['qmp'].append({'node': node, 'command': command, 'response': response})
        save()
        return response['return']

    save()
    with contextlib.ExitStack() as stack:
        agent = stack.enter_context(contextlib.closing(paramiko.Agent()))
        clients, transports = {}, {}
        for node, host in [(0, '10.10.0.235'), (3, '10.10.0.232')]:
            transport = stack.enter_context(connect(SimpleNamespace(host=host, username='root'), agent.get_keys()[0]))
            client = stack.enter_context(paramiko.SFTPClient.from_transport(transport))
            assert run(transport, '/usr/bin/locally /usr/bin/uname -n') == (0, 'wos-' + str(node), '')
            try:
                client.stat(helper)
            except FileNotFoundError:
                pass
            else:
                raise RuntimeError('Refusing to overwrite ' + helper)
            client.put(args.binary, helper)
            client.chmod(helper, 0o755)
            transports[node], clients[node] = transport, client
        gdb_socket = str(args.gdb_socket.resolve() if args.gdb_socket else root / 'receiver-gdb.sock')
        if args.gdb_socket:
            assert Path(gdb_socket).is_socket()
            qmp(3, 'query-status')
        else:
            assert not Path(gdb_socket).exists()
            reply = qmp(3, 'human-monitor-command', {'command-line': 'gdbserver unix:' + gdb_socket + ',server=on,wait=off'})
            assert 'failed' not in reply.lower(), reply
        try:
            if args.worker_exec:
                for uid in [0, 104]:
                    directory = f'{helper}-{uid}'
                    clients[3].mkdir(directory, 0o700)
                    clients[3].chown(directory, uid, uid)
                    metadata = clients[3].stat(directory)
                    assert metadata.st_uid == uid and metadata.st_gid == uid and metadata.st_mode & 0o777 == 0o700
            for uid in [0, 104]:
                case_dir = root / str(uid)
                case_dir.mkdir()
                config = {'root': str(case_dir), 'source_qmp': targets[0]['qmpSocket'], 'helper_name': Path(helper).name, 'commit_address': commit_address}
                (case_dir / 'config.json').write_text(json.dumps(config))
                # A hardware breakpoint observes the existing commit entry; it
                # neither rewrites kernel text nor changes a protocol packet.
                script = case_dir / 'break.py'
                script.write_text('''import gdb,json,socket,time
from pathlib import Path
c=json.loads(Path(CONFIG).read_text());r=Path(c['root'])
b=gdb.Breakpoint('*'+hex(c['commit_address']),type=gdb.BP_HARDWARE_BREAKPOINT)
(r/'armed').write_text('ready')
gdb.execute('continue')
t=gdb.parse_and_eval('$rdi').cast(gdb.lookup_type('ker::mod::sched::task::Task').pointer()).dereference();h=int(gdb.parse_and_eval('$rsi'))
name=t['name'].string();assert name==c['helper_name'],name
hit={'pid':int(t['pid']),'task':int(t.address),'handle':h,'name':name,'time':time.monotonic()}
with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as s:
 s.settimeout(5);s.connect(c['source_qmp'])
 with s.makefile('rwb') as f:
  json.loads(f.readline())
  for cmd in ['qmp_capabilities','stop']:
   f.write((json.dumps({'execute':cmd})+'\\n').encode());f.flush()
   while True:
    a=json.loads(f.readline())
    if 'return' in a or 'error' in a:assert 'error' not in a;break
(r/'hit.json').write_text(json.dumps(hit))
b.delete()
gdb.execute('detach')
'''.replace('CONFIG', repr(str(case_dir / 'config.json'))))
                observe = case_dir / 'observe.py'
                observe.write_text('''import gdb,json
from pathlib import Path
r=Path(ROOT);hit=json.loads((r/'hit.json').read_text())
a=gdb.parse_and_eval("'ker::net::wki::(anonymous namespace)::s_child_queries'")
t=a.type.strip_typedefs().template_argument(0);p=a.address.cast(t.pointer())
out={'found':False}
for i in range(a.type.sizeof//t.sizeof):
 row=p[i]
 if bool(row['active']) and int(row['token'])==hit['handle'] and int(row['native_waiter'])==hit['task']:
  task=row['native_waiter'].dereference()
  owner=row['owner'].dereference()
  out={'found':True,'phase':int(row['native_phase']),'busy':bool(row['busy']),'retiring':bool(row['retiring']),
       'detached':bool(row['native_detached']),'pid':int(task['pid']),
       'is_thread':bool(task['is_thread']),'owner_pid':int(row['owner'].dereference()['pid']),
       'owner':int(owner.address),'owner_root':int(owner['pagemap']),'waiter_root':int(task['pagemap']),
       'owner_imported_user':int(owner['imported_child_user_time_us']),'owner_imported_system':int(owner['imported_child_system_time_us']),
       'claimed_user':int(row['native_result']['user_time_us']),'claimed_system':int(row['native_result']['system_time_us']),
       'owner_exiting':bool(int.from_bytes(bytes(gdb.selected_inferior().read_memory(int(owner['exit_in_progress'].address), owner['exit_in_progress'].type.sizeof)), 'little')),
       'exit_in_progress':bool(int.from_bytes(bytes(gdb.selected_inferior().read_memory(int(task['exit_in_progress'].address), task['exit_in_progress'].type.sizeof)), 'little')),
       'claim_action':str(row['claim_action']),'claim_depth':str(row['claim_depth'])}
  break
(r/OUTPUT).write_text(json.dumps(out,indent=2))
gdb.execute('detach')
'''.replace('ROOT', repr(str(case_dir))))
                base = ['gdb', '-q', '-batch', args.kernel, '-ex', 'set pagination off', '-ex', 'set confirm off',
                        '-ex', 'set max-value-size 262144', '-ex', 'target remote ' + gdb_socket]
                log = stack.enter_context((case_dir / 'break.log').open('w'))
                debugger = subprocess.Popen(base + ['-ex', 'source ' + str(script)], stdout=log, stderr=subprocess.STDOUT)
                channel = None
                case = {'uid': uid}
                result['cases'].append(case)
                if args.receiver_reset:
                    case['peer_before'] = snapshot(transports[0], case_dir, 'before', 'wos-3')
                save()
                try:
                    deadline = time.monotonic() + 15
                    while not (case_dir / 'armed').exists():
                        assert debugger.poll() is None, 'Debugger failed before arming'
                        assert time.monotonic() < deadline, 'Debugger arm timeout'
                        time.sleep(0.02)
                    channel = transports[0].open_session(timeout=10)
                    channel.settimeout(30)
                    command = shlex.join(['/usr/bin/locally', helper, str(uid), 'wos-3', '/wki/wos-0' + helper]
                                         + (['--receiver-reset'] if args.receiver_reset else ['--worker-exec'] if args.worker_exec else ['--worker-exit'] if args.worker_exit else []))
                    channel.exec_command(command)
                    debugger.wait(timeout=15)
                    assert debugger.returncode == 0 and (case_dir / 'hit.json').exists()
                    hit = json.loads((case_dir / 'hit.json').read_text())
                    case['hit'] = hit
                    assert not qmp(0, 'query-status')['running']
                    time.sleep(0.03)
                    def inspect(label):
                        text = observe.read_text().replace('OUTPUT', repr(label + '.json'))
                        path = case_dir / (label + '.py')
                        path.write_text(text)
                        with (case_dir / (label + '.log')).open('w') as output:
                            done = subprocess.run(base + ['-ex', 'source ' + str(path)], stdout=output, stderr=subprocess.STDOUT, timeout=10)
                        assert done.returncode == 0, label
                        return json.loads((case_dir / (label + '.json')).read_text())
                    before = inspect('before-kill')
                    case['before'] = before
                    save()
                    assert before['found'] and before['phase'] == 5 and not before['exit_in_progress'] and not before['retiring'], before
                    assert before['is_thread'] == worker, before
                    assert (before['owner_pid'] != hit['pid']) == worker, before
                    if args.receiver_reset:
                        case['reset'] = qmp(3, 'system_reset')
                        save()
                    else:
                        started = time.monotonic()
                        killed = run(transports[3], shlex.join(['/usr/bin/locally', helper,
                                     '--exit' if worker else '--kill', str(before['owner_pid']), str(uid)]))
                        case['kill'] = killed
                        save()
                        assert killed[0] == 0 and not killed[2], killed
                        if args.worker_exec:
                            deadline = time.monotonic() + 5
                            while True:
                                try:
                                    with clients[3].open(f'{helper}-{uid}/ready') as ready:
                                        case['replacement_ready'] = ready.read().decode()
                                    break
                                except FileNotFoundError:
                                    assert time.monotonic() < deadline, 'Replacement did not publish ready marker'
                                    time.sleep(0.02)
                            assert case['replacement_ready'] == f"READY pid={before['owner_pid']} uid={uid}\n"
                        time.sleep(0.05)
                        after = inspect('after-kill')
                        case['after'] = after
                        case['termination_observation_ms'] = (time.monotonic() - started) * 1000
                        try:
                            clients[3].stat('/proc/' + str(hit['pid']) + '/stat')
                            case['receiver_absent_while_source_paused'] = False
                        except FileNotFoundError:
                            case['receiver_absent_while_source_paused'] = True
                        save()
                        assert case['termination_observation_ms'] < 2000
                        assert case['receiver_absent_while_source_paused'] or (after['found'] and after['exit_in_progress']), after
                        if args.worker_exec:
                            assert after['found'] and after['detached'] and not after['retiring'] and not after['owner_exiting'], after
                            assert after['owner'] == before['owner'] and after['owner_root'] != before['owner_root'], after
                            assert after['owner_imported_user'] == before['owner_imported_user']
                            assert after['owner_imported_system'] == before['owner_imported_system']
                finally:
                    if debugger.poll() is None:
                        debugger.terminate()
                        debugger.wait(timeout=5)
                    # Detach/error may leave a VM stopped. Resume both owned guests.
                    for node in [0, 3]:
                        qmp(node, 'cont')
                if args.receiver_reset:
                    case['peer_after'] = wait_snapshot(transports[0], case_dir, 'after', 'wos-3',
                        lambda state: state['epoch'] != 0 and state['epoch'] != case['peer_before']['epoch']
                        and any(p['mount'] == '/wki/wos-3' for p in state['active_proxies']))
                    deadline = time.monotonic() + 60
                    while True:
                        try:
                            transport = stack.enter_context(connect(SimpleNamespace(host='10.10.0.232', username='root'), agent.get_keys()[0]))
                            client = stack.enter_context(paramiko.SFTPClient.from_transport(transport))
                            assert run(transport, '/usr/bin/locally /usr/bin/uname -n') == (0, 'wos-3', '')
                            transports[3], clients[3] = transport, client
                            break
                        except (OSError, paramiko.SSHException, AssertionError):
                            assert time.monotonic() < deadline, 'Receiver SSH did not return after reset'
                            time.sleep(1)
                    case['receiver_incarnation_ended'] = True
                    save()
                assert channel is not None
                if args.worker_exec:
                    deadline = time.monotonic() + 5
                    for attempt in range(30):
                        completion = inspect('completion-' + str(attempt))
                        if not completion['found']:
                            break
                        assert time.monotonic() < deadline, 'Detached continuation did not retire'
                        time.sleep(0.03)
                    assert not completion['found'], completion
                    # The replacement is held by its own local release marker;
                    # validate its exact owner identity before reading counters.
                    owner_script = case_dir / 'owner-accounting.py'
                    owner_script.write_text('''import gdb,json
from pathlib import Path
b=json.loads(Path(BEFORE).read_text())
t=gdb.Value(b['owner']).cast(gdb.lookup_type('ker::mod::sched::task::Task').pointer()).dereference()
assert int(t['pid'])==b['owner_pid'] and t['name'].string()==NAME
out={'pid':int(t['pid']),'root':int(t['pagemap']),
     'user':int(t['imported_child_user_time_us']),'system':int(t['imported_child_system_time_us'])}
Path(DEST).write_text(json.dumps(out,indent=2))
gdb.execute('detach')
'''.replace('BEFORE', repr(str(case_dir / 'before-kill.json')))
                        .replace('NAME', repr(Path(helper).name)).replace('DEST', repr(str(case_dir / 'owner-accounting.json'))))
                    with (case_dir / 'owner-accounting.log').open('w') as output:
                        done = subprocess.run(base + ['-ex', 'source ' + str(owner_script)], stdout=output,
                                              stderr=subprocess.STDOUT, timeout=10)
                    assert done.returncode == 0
                    accounting = json.loads((case_dir / 'owner-accounting.json').read_text())
                    case['accounting'] = accounting
                    save()
                    assert before['claimed_user'] + before['claimed_system'] > 0
                    assert accounting['user'] == before['owner_imported_user'] + before['claimed_user']
                    assert accounting['system'] == before['owner_imported_system'] + before['claimed_system']
                    assert accounting['root'] == after['owner_root']
                    with clients[3].open(f'{helper}-{uid}/release', 'wx') as release:
                        release.write('release\n')
                try:
                    output = bytearray()
                    while chunk := channel.recv(4096):
                        output.extend(chunk)
                        assert len(output) < 32768
                    stderr = channel.recv_stderr(4096).decode()
                    status = channel.recv_exit_status()
                finally:
                    channel.close()
                case.update(status=status, stdout=output.decode(), stderr=stderr)
                save()
                lines = case['stdout'].splitlines()
                assert status == 0 and not stderr and len(lines) == 2 and lines[0].startswith('CREATED ') and lines[1].startswith('PASS '), case
                fields = dict(x.split('=', 1) for x in lines[1].split()[1:])
                case['probes'] = []
                assert int(fields['status']) == (255 << 8 if args.receiver_reset else 23 << 8 if worker else 9), fields
                identities = [(0, int(fields['parent_source'])), (0, int(fields['child_source'])), (3, hit['pid'])]
                if args.receiver_reset:
                    identities = identities[:2]
                if worker:
                    identities.append((3, before['owner_pid']))
                for node, pid in identities:
                    deadline = time.monotonic() + 5
                    while True:
                        try:
                            clients[node].stat(f'/proc/{pid}/stat')
                        except FileNotFoundError:
                            case['probes'].append({'node': node, 'pid': pid, 'absent': True})
                            break
                        assert time.monotonic() < deadline, (node, pid)
                        time.sleep(0.05)
                save()
                if args.worker_exec:
                    with clients[3].open(f'{helper}-{uid}/done') as done:
                        case['replacement_result'] = done.read().decode()
                    assert case['replacement_result'].startswith(f"PASS pid={before['owner_pid']} uid={uid} "), case
                    save()
                print(json.dumps(case), flush=True)
        finally:
            for node in [0, 3]:
                qmp(node, 'cont')
            for node, client in clients.items():
                if node == 3 and args.worker_exec:
                    for uid in [0, 104]:
                        for suffix in ['ready', 'release', 'done']:
                            path = f'{helper}-{uid}/{suffix}'
                            try:
                                client.remove(path)
                                result['markers_removed'].append(path)
                            except FileNotFoundError:
                                pass
                        try:
                            client.rmdir(f'{helper}-{uid}')
                            result['markers_removed'].append(f'{helper}-{uid}')
                        except FileNotFoundError:
                            pass
                try:
                    client.remove(helper)
                except FileNotFoundError:
                    assert node == 3 and args.receiver_reset
                result['removed'].append(node)
                save()


if __name__ == '__main__':
    main()
