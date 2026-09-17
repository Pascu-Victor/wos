#!/usr/bin/env python3
"""Ordinary owned signal RPCs interrupted at reply enqueue by receiver reset."""
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
    for name in ['binary', 'kernel']:
        parser.add_argument('--' + name, required=True)
    for name in ['live', 'output']:
        parser.add_argument('--' + name, required=True, type=Path)
    parser.add_argument('--gdb-root', type=Path, help='Existing exact-guest listener directory')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    root = args.output.resolve()
    targets = {int(t['nodeId']): t for t in json.loads(args.live.read_text())['targets']}
    helper = '/tmp/goal19-signal-reply-' + str(time.time_ns())
    symbol = '_ZN3ker3net3wki30wki_send_on_channel_generationEtPNS1_10WkiChannelEjNS1_7MsgTypeEPKvt'
    matches = [line.split()[0] for line in subprocess.check_output(['nm', args.kernel], text=True).splitlines()
               if line.endswith(' ' + symbol)]
    assert len(matches) == 1
    address = int(matches[0], 16)
    result = {'helper': helper, 'cases': [], 'qmp': [], 'removed': False}

    def save():
        (root / 'results.json').write_text(json.dumps(result, indent=2) + '\n')

    def qmp(node, command, arguments=None):
        t = targets[node]
        proc = Path('/proc') / str(t['pid'])
        assert (proc / 'stat').read_text().rsplit(')', 1)[1].split()[19] == t['processStartTicks']
        assert t['qmpSocket'] in (proc / 'cmdline').read_bytes().decode()
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect(t['qmpSocket'])
            with s.makefile('rwb') as f:
                json.loads(f.readline())
                for cmd, body in [('qmp_capabilities', None), (command, arguments)]:
                    f.write((json.dumps({'execute': cmd, **({'arguments': body} if body else {})}) + '\n').encode())
                    f.flush()
                    while True:
                        reply = json.loads(f.readline())
                        if 'return' in reply or 'error' in reply:
                            assert 'error' not in reply, reply
                            break
        result['qmp'].append({'node': node, 'command': command, 'response': reply})
        save()
        return reply['return']

    def debugger(node, script):
        return ['gdb', '-q', '-batch', args.kernel, '-ex', 'set pagination off', '-ex', 'set confirm off',
                '-ex', 'set max-value-size 262144', '-ex', 'target remote ' + str((args.gdb_root.resolve() if args.gdb_root else root) / f'gdb-{node}.sock'),
                '-ex', 'source ' + str(script)]

    save()
    with contextlib.ExitStack() as stack:
        agent = stack.enter_context(contextlib.closing(paramiko.Agent()))
        transport = stack.enter_context(connect(SimpleNamespace(host='10.10.0.235', username='root'), agent.get_keys()[0]))
        client = stack.enter_context(paramiko.SFTPClient.from_transport(transport))
        assert run(transport, '/usr/bin/locally /usr/bin/uname -n') == (0, 'wos-0', '')
        try:
            client.stat(helper)
        except FileNotFoundError:
            pass
        else:
            raise RuntimeError('Refusing overwrite ' + helper)
        client.put(args.binary, helper)
        client.chmod(helper, 0o755)
        try:
            for node in [0, 3]:
                if args.gdb_root:
                    assert (args.gdb_root / f'gdb-{node}.sock').is_socket()
                    qmp(node, 'query-status')
                    continue
                path = root / f'gdb-{node}.sock'
                assert not path.exists()
                reply = qmp(node, 'human-monitor-command', {'command-line': 'gdbserver unix:' + str(path) + ',server=on,wait=off'})
                assert 'failed' not in reply.lower(), reply
            for index, (caller, target, signum) in enumerate([(0, 0, 10), (104, 104, 10), (104, 200, 10), (104, 200, 0)]):
                directory = root / str(index)
                directory.mkdir()
                case = {'caller': caller, 'target': target, 'signum': signum}
                result['cases'].append(case)
                case['peer_before'] = snapshot(transport, directory, 'before', 'wos-3')
                with transport.open_session(timeout=10) as channel:
                    channel.settimeout(90)
                    channel.exec_command(shlex.join(['/usr/bin/locally', helper, 'wos-3', '/wki/wos-0' + helper,
                                                    str(caller), str(target), str(signum)]))
                    reader = channel.makefile('r')
                    ready = reader.readline().strip()
                    case['ready'] = ready
                    save()
                    assert ready.startswith('READY '), ready
                    fields = dict(x.split('=', 1) for x in ready.split()[1:])
                    assert int(fields['caller']) == caller and int(fields['target']) == target and int(fields['signum']) == signum
                    config = {'targets': targets, 'path': str(directory), 'address': address, 'receiver': int(fields['receiver']),
                              'expected': 0 if caller == 0 or caller == target else -1, 'signum': signum, 'name': Path(helper).name}
                    (directory / 'config.json').write_text(json.dumps(config))
                    script = directory / 'reply.py'
                    script.write_text('''import gdb,json,socket
from pathlib import Path
c=json.loads(Path(CONFIG).read_text());r=Path(c['path'])
def qmp(node,command):
 t=c['targets'][str(node)];p=Path('/proc')/str(t['pid'])
 assert (p/'stat').read_text().rsplit(')',1)[1].split()[19]==t['processStartTicks']
 assert t['qmpSocket'] in (p/'cmdline').read_bytes().decode()
 with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as s:
  s.settimeout(5);s.connect(t['qmpSocket'])
  with s.makefile('rwb') as f:
   json.loads(f.readline())
   for cmd in ['qmp_capabilities',command]:
    f.write((json.dumps({'execute':cmd})+'\\n').encode());f.flush()
    while True:
     a=json.loads(f.readline())
     if 'return' in a or 'error' in a:assert 'error' not in a;break
 return a
b=gdb.Breakpoint('*'+hex(c['address']),type=gdb.BP_HARDWARE_BREAKPOINT)
b.condition='$rcx == 0x59'
(r/'armed').write_text('ready')
gdb.execute('continue')
assert int(gdb.parse_and_eval('$rcx'))==0x59 and int(gdb.parse_and_eval('$r9'))==16
reply=gdb.parse_and_eval('$r8').cast(gdb.lookup_type('ker::net::wki::TaskSignalResponse').pointer()).dereference()
out={'request':int(reply['request_id']),'task_id':int(reply['task_id']),'result':int(reply['result']),
     'node':int(gdb.parse_and_eval('$rdi')),'generation':int(gdb.parse_and_eval('$rdx'))}
a=gdb.parse_and_eval("'ker::net::wki::(anonymous namespace)::g_running_remote_tasks'")
t=a.type.strip_typedefs().template_argument(0);inline=a['m_inline'].address.cast(t.pointer());found=[]
for i in range(int(a['m_size'])):
 row=inline[i] if i<16 else a['m_heap'][i-16]
 if bool(row['active']) and int(row['task_id'])==out['task_id'] and int(row['submitter_node'])==out['node']:
  task=row['task'].dereference()
  assert int(task['pid'])==c['receiver'] and task['name'].string()==c['name']
  pending=int.from_bytes(bytes(gdb.selected_inferior().read_memory(int(task['sig_pending'].address),8)),'little')
  found.append({'pid':int(task['pid']),'pending':pending,'published':bool(row['published'])})
assert len(found)==1 and found[0]['published']
out['target']=found[0];assert out['result']==c['expected']
assert bool(found[0]['pending'] & (1<<9)) == (c['signum']==10 and c['expected']==0)
(r/'reply.json').write_text(json.dumps(out,indent=2))
qmp(0,'stop');b.delete()
out['reset']=qmp(3,'system_reset');(r/'reply.json').write_text(json.dumps(out,indent=2))
gdb.execute('detach')
'''.replace('CONFIG', repr(str(directory / 'config.json'))))
                    log = stack.enter_context((directory / 'reply.log').open('w'))
                    process = subprocess.Popen(debugger(3, script), stdout=log, stderr=subprocess.STDOUT)
                    try:
                        deadline = time.monotonic() + 15
                        while not (directory / 'armed').exists():
                            assert process.poll() is None and time.monotonic() < deadline
                            time.sleep(0.02)
                        channel.sendall(b'G')
                        process.wait(timeout=20)
                        assert process.returncode == 0 and (directory / 'reply.json').exists()
                        case['reply'] = json.loads((directory / 'reply.json').read_text())
                        assert 'reset' in case['reply']
                        assert not qmp(0, 'query-status')['running']
                        # Keep the source's pending operation paused until the
                        # replacement guest has booted, separating peer retirement
                        # from the ordinary RPC deadline race in the initial run.
                        deadline = time.monotonic() + 15
                        while True:
                            try:
                                with connect(SimpleNamespace(host='10.10.0.232', username='root'), agent.get_keys()[0]) as replacement:
                                    assert run(replacement, '/usr/bin/locally /usr/bin/uname -n') == (0, 'wos-3', '')
                                case['replacement_ssh_ready_before_source_resume'] = True
                                save()
                                break
                            except (OSError, paramiko.SSHException, AssertionError):
                                assert time.monotonic() < deadline, 'Replacement receiver did not boot'
                                time.sleep(0.2)
                        pending = directory / 'pending.py'
                        pending.write_text('''import gdb,json
from pathlib import Path
r=Path(ROOT);reply=json.loads((r/'reply.json').read_text());out=[]
a=gdb.parse_and_eval("'ker::net::wki::(anonymous namespace)::s_user_signals'")
t=a.type.strip_typedefs().template_argument(0);p=a.address.cast(t.pointer())
for i in range(a.type.sizeof//t.sizeof):
 row=p[i]
 if int(row['owner']) and int(row['request_id'])==reply['request'] and int(row['task_id'])==reply['task_id']:
  owner=row['owner'].dereference();assert owner['name'].string()==NAME
  out.append({'owner_pid':int(owner['pid']),'node':int(row['node']),'generation':int(row['generation']),
              'response_wait':int(row['response_wait']),'consumer_wait':int(row['consumer_wait']),'result':int(row['result'])})
assert len(out)==1 and out[0]['node']==4 and out[0]['response_wait'] and out[0]['consumer_wait']
(r/'pending.json').write_text(json.dumps(out[0],indent=2))
gdb.execute('detach')
'''.replace('ROOT', repr(str(directory))).replace('NAME', repr(Path(helper).name)))
                        with (directory / 'pending.log').open('w') as log_pending:
                            checked = subprocess.run(debugger(0, pending), stdout=log_pending, stderr=subprocess.STDOUT, timeout=10)
                        assert checked.returncode == 0
                        case['pending'] = json.loads((directory / 'pending.json').read_text())
                        save()
                    finally:
                        if process.poll() is None:
                            process.terminate()
                            process.wait(timeout=5)
                        for node in [0, 3]:
                            qmp(node, 'cont')
                    case['final'] = reader.readline().strip()
                    case['status'] = channel.recv_exit_status()
                    case['stderr'] = channel.recv_stderr(8192).decode()
                    save()
                    assert case['final'].startswith('PASS ') and case['status'] == 0 and not case['stderr'], case
                    final = dict(x.split('=', 1) for x in case['final'].split()[1:])
                    assert int(final['sender']) == case['pending']['owner_pid']
                    case['peer_after'] = wait_snapshot(transport, directory, 'after', 'wos-3',
                        lambda s: s['epoch'] and s['epoch'] != case['peer_before']['epoch']
                        and any(p['mount'] == '/wki/wos-3' for p in s['active_proxies']))
                    case['probes'] = []
                    for pid in [int(final['child']), int(final['sender'])]:
                        try:
                            client.stat(f'/proc/{pid}/stat')
                        except FileNotFoundError:
                            case['probes'].append({'node': 0, 'pid': pid, 'absent': True})
                        else:
                            raise AssertionError(pid)
                    save()
                    print(index, caller, target, signum, case['final'], flush=True)
        finally:
            for node in [0, 3]:
                qmp(node, 'cont')
            client.remove(helper)
            result['removed'] = True
            save()


if __name__ == '__main__':
    main()
