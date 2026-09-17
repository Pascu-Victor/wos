#!/usr/bin/env python3
"""Compare parent-death registration/delivery on owned local and remote descendants."""
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
    parser.add_argument('--extended', action='store_true')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    helper = '/tmp/goal19-parent-death-' + str(time.time_ns())
    result = {'helper': helper, 'rows': [], 'probes': [], 'removed': []}

    def save():
        (args.output / 'results.json').write_text(json.dumps(result, indent=2) + '\n')

    save()
    with contextlib.ExitStack() as stack:
        agent = stack.enter_context(contextlib.closing(paramiko.Agent()))
        transports = {}
        clients = {}
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
        command = shlex.join(['/usr/bin/locally', helper, 'wos-3', '/wki/wos-0' + helper] + (['--extended'] if args.extended else []))
        status, output, error = run(transports['wos-0'], command)
        (args.output / 'stdout.txt').write_text(output + '\n')
        (args.output / 'stderr.txt').write_text(error)
        rows = [line for line in output.splitlines() if line.startswith(('PASS ', 'FAIL '))]
        result.update(command=command, status=status, stderr=error, rows=rows)
        save()
        print(output, flush=True)
        assert len(rows) == (30 if args.extended else 8)
        for line in rows:
            fields = dict(item.split('=', 1) for item in line.split()[1:])
            assert all(fields[k] == '1' for k in ['ready', 'parent_reaped', 'target_finished', 'source_absent']), line
            assert fields['parent_status'] == '0'
            node, pid = fields['target_node'], fields['target_pid']
            probe = run(transports[node], shlex.join(['/usr/bin/locally', helper, '--gone', pid]))
            result['probes'].append({'node': node, 'pid': int(pid), 'result': probe})
            save()
            assert probe == (0, '', ''), (node, pid, probe)
            if int(fields.get('hop_pid', 0)):
                hop = run(transports['wos-3'], shlex.join(['/usr/bin/locally', helper, '--gone', fields['hop_pid']]))
                result['probes'].append({'node': 'wos-3', 'pid': int(fields['hop_pid']), 'result': hop})
                save()
                assert hop == (0, '', ''), hop
        for node, sftp in clients.items():
            sftp.remove(helper)
            result['removed'].append(node)
            save()
        print('All direct parents reaped, source/receiver procfs entries absent, both helpers removed.', flush=True)
    if status or error or any(row.startswith('FAIL ') for row in rows):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
