#!/usr/bin/env python3
"""Check exact group wait selection with colliding imported and local PGIDs."""
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
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    helper = '/tmp/goal19-wait-group-origin-' + str(time.time_ns())
    result = {'helper': helper, 'rows': [], 'probes': [], 'removed': []}

    def save():
        (args.output / 'results.json').write_text(json.dumps(result, indent=2) + '\n')

    save()
    with contextlib.ExitStack() as stack:
        agent = stack.enter_context(contextlib.closing(paramiko.Agent()))
        transports, clients = {}, {}
        for node, host in [('wos-0', '10.10.0.235'), ('wos-3', '10.10.0.232')]:
            transport = stack.enter_context(connect(SimpleNamespace(host=host, username='root'), agent.get_keys()[0]))
            sftp = stack.enter_context(paramiko.SFTPClient.from_transport(transport))
            assert run(transport, '/usr/bin/locally /usr/bin/uname -n') == (0, node, '')
            try:
                sftp.stat(helper)
            except FileNotFoundError:
                pass
            else:
                raise RuntimeError('Refusing to overwrite existing helper: ' + helper)
            sftp.put(args.binary, helper)
            sftp.chmod(helper, 0o755)
            transports[node], clients[node] = transport, sftp
        command = shlex.join(['/usr/bin/locally', helper, 'wos-3', '/wki/wos-0' + helper])
        status, output, error = run(transports['wos-0'], command)
        (args.output / 'stdout.txt').write_text(output + '\n')
        (args.output / 'stderr.txt').write_text(error)
        rows = [line for line in output.splitlines() if line.startswith(('PASS ', 'FAIL '))]
        result.update(command=command, status=status, stderr=error, rows=rows)
        save()
        print(output, flush=True)
        assert len(rows) == 2
        identities = set()
        for line in rows:
            fields = dict(item.split('=', 1) for item in line.split()[1:])
            assert fields['setup'] == fields['parent_reaped'] == fields['cleaned'] == '1', line
            assert fields['parent_status'] == '0', line
            identities.update([('wos-0', int(fields['parent_source']), 'parent-source'),
                               ('wos-3', int(fields['parent_receiver']), 'parent-receiver'),
                               ('wos-3', int(fields['local']), 'local-child'),
                               ('wos-3', int(fields['foreign']), 'foreign-child')])
        # Record retained zombies as evidence, never silently call them cleaned.
        # These are the fixture's own recorded PIDs, queried on their physical node.
        for node, pid, role in sorted(identities):
            name = f'/proc/{pid}/stat'
            try:
                with clients[node].open(name, 'r') as stream:
                    contents = stream.read().decode()
                probe = {'node': node, 'pid': pid, 'role': role, 'present': True, 'stat': contents}
                with clients[node].open(f'/proc/{pid}/status', 'r') as stream:
                    probe['status'] = stream.read().decode()
            except FileNotFoundError:
                probe = {'node': node, 'pid': pid, 'role': role, 'present': False}
            result['probes'].append(probe)
            save()
        for node, sftp in clients.items():
            sftp.remove(helper)
            result['removed'].append(node)
            save()
        present = [probe for probe in result['probes'] if probe['present']]
        print('Both helper files removed; retained process entries:', json.dumps(present), flush=True)
    if status or error or any(row.startswith('FAIL ') for row in rows) or present:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
