#!/usr/bin/env python3
"""Qualify exact service-profile denials on owned WOS nodes and remote execution."""
import argparse
import contextlib
import json
from pathlib import Path
import shlex
import time
from types import SimpleNamespace

import paramiko
from service_ssh_broker_lifetime import connect
from service_wki_identity import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True)
    parser.add_argument('--receiver', required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(exist_ok=False, parents=True)
    suffix = str(time.time_ns())
    helper = '/tmp/goal19-service-roles-' + suffix
    key = '/root/.ssh/goal19-service-roles-' + suffix
    rows = []
    with contextlib.closing(paramiko.Agent()) as agent, contextlib.ExitStack() as stack:
        clients = []
        for host in (args.source, args.receiver):
            transport = stack.enter_context(connect(SimpleNamespace(host=host, username='root'), agent.get_keys()[0]))
            sftp = stack.enter_context(paramiko.SFTPClient.from_transport(transport))
            clients.append((transport, sftp))
        try:
            for _, sftp in clients:
                with sftp.open(key, 'wx') as file:
                    file.write('ordinary authorization fixture; no key material\n')
                sftp.chmod(key, 0o600)
                metadata = sftp.stat(key)
                assert metadata.st_uid == 0 and metadata.st_gid == 0 and metadata.st_mode & 0o777 == 0o600
                sftp.put(str(args.binary), helper)
                sftp.chmod(helper, 0o755)
            commands = [
                ('local-source', 0, ['/usr/bin/locally', helper, key, 'wos-0']),
                ('local-receiver', 1, ['/usr/bin/locally', helper, key, 'wos-3']),
                ('remote-receiver', 0, ['/usr/bin/on', 'wos-3', '/wki/wos-0' + helper, key, 'wos-3']),
            ]
            for name, index, command in commands:
                result = run(clients[index][0], shlex.join(command))
                rows.append({'name': name, 'command': command, 'result': result})
                (args.output / 'results.json').write_text(json.dumps(rows, indent=2) + '\n')
                print(name, result, flush=True)
                assert result[0] == 0 and not result[2] and 'FAIL' not in result[1]
                assert result[1].count(' checks=14 failed=0') == 5
                assert 'PASS service-role profiles=5 checks=70 failures=0 target-reaped=1' in result[1]
        finally:
            for _, sftp in clients:
                for path in (helper, key):
                    try:
                        sftp.remove(path)
                    except FileNotFoundError:
                        pass
        for _, sftp in clients:
            for path in (helper, key):
                try:
                    sftp.stat(path)
                except FileNotFoundError:
                    continue
                raise AssertionError('fixture remains: ' + path)
    print('PASS: 210 service-profile denial checks; all owned targets reaped and fixture files removed', flush=True)


if __name__ == '__main__':
    main()
