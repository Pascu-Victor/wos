#!/usr/bin/env python3
"""Observe /dev/nodes while syncing and stopping the owned peer VM named wos-3."""
import argparse
import contextlib
import json
import shlex
import socket
import time
from pathlib import Path

import paramiko
from service_ssh_broker_lifetime import connect
from service_wki_identity import run


def stop_owned_peer(descriptor):
    data = json.loads(descriptor.read_text())
    target = next(t for t in data['targets'] if t['nodeId'] == 3 and t['hostname'] == 'wos-3')
    proc = Path('/proc', str(target['pid']))
    assert proc.joinpath('stat').read_text().rsplit(')', 1)[1].split()[19] == target['processStartTicks']
    overlay = str(descriptor.parent / 'disk-vm3.qcow2').encode()
    assert overlay in proc.joinpath('cmdline').read_bytes()
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(5)
        sock.connect(target['qmpSocket'])
        stream = sock.makefile('rwb', buffering=0)
        json.loads(stream.readline())
        for command in ('qmp_capabilities', 'quit'):
            stream.write((json.dumps({'execute': command}) + '\r\n').encode())
            while True:
                raw = stream.readline()
                if not raw and command == 'quit':
                    break
                row = json.loads(raw)
                if 'error' in row:
                    raise RuntimeError(row)
                if 'return' in row:
                    break
    return {'pid': target['pid'], 'startTicks': target['processStartTicks'], 'command': 'quit'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--descriptor', type=Path, required=True)
    parser.add_argument('--peer-sync-binary', type=Path, required=True)
    args = parser.parse_args()
    result = {}
    with contextlib.closing(paramiko.Agent()) as agent, connect(args, agent.get_keys()[0]) as transport:
        helper = '/tmp/goal19-devfs-peer-' + str(time.time_ns())
        sync_helper = helper + '-sync'
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.put(str(args.binary), helper)
            sftp.chmod(helper, 0o755)
            sftp.put(str(args.peer_sync_binary), sync_helper)
            sftp.chmod(sync_helper, 0o755)
            try:
                with transport.open_session(timeout=10) as channel:
                    channel.settimeout(60)
                    channel.exec_command('/usr/bin/locally ' + shlex.quote(helper))
                    output = b''
                    while b'READY peer=wos-3 retained=id,directory\n' not in output:
                        chunk = channel.recv(4096)
                        if not chunk or len(output) > 4096:
                            raise RuntimeError('native fixture did not become ready: ' + repr(output))
                        output += chunk
                    status, out, err = run(transport, '/usr/bin/locally ' + shlex.quote(sync_helper) + ' --peer-sync wos-3')
                    result['peer_sync'] = {'status': status, 'stdout': out, 'stderr': err}
                    assert status == 0 and not out and not err, result
                    result['qmp_stop'] = stop_owned_peer(args.descriptor)
                    while chunk := channel.recv(4096):
                        output += chunk
                        if len(output) > 8192:
                            raise RuntimeError('unexpected fixture output')
                    result['native'] = {'status': channel.recv_exit_status(), 'stdout': output.decode(),
                                        'stderr': channel.recv_stderr(4096).decode()}
                    assert result['native']['status'] == 0 and 'PASS peer=wos-3 absent' in output.decode(), result
            finally:
                sftp.remove(helper)
                sftp.remove(sync_helper)
                result['helper_removed'] = True
                args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
