#!/usr/bin/env python3
"""Exercise owned descriptor controls and local epoll/futex authority across WKI exec."""
import argparse, contextlib, json, re, shlex, time
from pathlib import Path
import paramiko
from service_ssh_broker_lifetime import connect
from service_wki_identity import run


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('host'); p.add_argument('--username', default='root')
    p.add_argument('--binary', required=True); p.add_argument('--peer', default='wos-3')
    p.add_argument('--owner', default='wos-0'); p.add_argument('--output', type=Path, required=True)
    a = p.parse_args(); a.output.mkdir(parents=True, exist_ok=False)
    helper = '/tmp/goal19-ipc-control-' + str(time.time_ns()); rows = []
    (a.output/'session.json').write_text(json.dumps({'helper': helper})+'\n')
    with contextlib.closing(paramiko.Agent()) as agent:
        with connect(a, agent.get_keys()[0]) as t, paramiko.SFTPClient.from_transport(t) as sftp:
            sftp.put(a.binary, helper); sftp.chmod(helper, 0o755)
            commands = []
            for uid in (0, 100):
                commands.append((f'socket-{uid}', ['/usr/bin/locally', helper, '--socket', a.peer, '/wki/'+a.owner+helper, str(uid)], False))
                for remote in (False, True):
                    host = a.peer if remote else a.owner
                    command = ['/usr/bin/on', host, '/wki/'+a.owner+helper] if remote else ['/usr/bin/locally', helper]
                    commands.append((f'pty-{host}-{uid}', command+['--pty', host, str(uid)], True))
                    target = a.owner if remote else a.peer
                    commands.append((f'local-{host}-{uid}', command+['--local', host, str(uid), target, '/wki/'+a.owner+helper], False))
            for name, args, pty in commands:
                command = shlex.join(args)
                with t.open_session(timeout=10) as channel:
                    channel.settimeout(60)
                    if pty: channel.get_pty(term='xterm', width=80, height=24)
                    channel.exec_command(command)
                    output = bytearray()
                    while chunk := channel.recv(4096):
                        output.extend(chunk)
                        if len(output) > 32768: raise RuntimeError('output bound')
                    error = channel.recv_stderr(4096).decode(); status = channel.recv_exit_status()
                text = output.decode().replace('\r\n', '\n')
                (a.output/(name+'.txt')).write_text(text)
                row = {'name':name, 'command':command, 'status':status, 'stderr':error}; rows.append(row)
                (a.output/'results.json').write_text(json.dumps(rows,indent=2)+'\n')
                print(name, status, text, flush=True)
                if status or error or 'FAIL' in text or 'PASS checks=' not in text: raise RuntimeError(name+' requires review')
                for pid, host in re.findall(r'RECEIVER pid=(\d+) host=(\S+)', text):
                    prefix = ['/usr/bin/locally'] if host == a.owner else ['/usr/bin/on', host]
                    body = 'test -d /proc/$$ || exit 3; test ! -d /proc/'+pid+' || exit 4'
                    result, out, err = run(t, shlex.join(prefix+['/usr/bin/sh','-c',body]))
                    row.setdefault('retirement',[]).append({'pid':pid,'host':host,'status':result,'stdout':out,'stderr':err})
                    (a.output/'results.json').write_text(json.dumps(rows,indent=2)+'\n')
                    if result or out or err: raise RuntimeError('receiver retirement requires review')
            sftp.remove(helper)
    print('All ten descriptor control cases passed; receivers retired and helper removed.', flush=True)

if __name__ == '__main__': main()
