#!/usr/bin/env python3
"""Verify owned-child CPU observations across local and two-hop remote exec."""
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
    helper = '/tmp/goal19-child-cpu-' + str(time.time_ns())
    result = {'helper': helper, 'cases': [], 'removed': []}
    def save():
        (args.output / 'results.json').write_text(json.dumps(result, indent=2) + '\n')
    save()
    with contextlib.ExitStack() as stack:
        agent = stack.enter_context(contextlib.closing(paramiko.Agent()))
        clients, transports = {}, {}
        for name, host in [('wos-0', '10.10.0.235'), ('wos-3', '10.10.0.232')]:
            transport = stack.enter_context(connect(SimpleNamespace(host=host, username='root'), agent.get_keys()[0]))
            sftp = stack.enter_context(paramiko.SFTPClient.from_transport(transport))
            assert run(transport, '/usr/bin/locally /usr/bin/uname -n') == (0, name, '')
            try:
                sftp.stat(helper)
            except FileNotFoundError:
                pass
            else:
                raise RuntimeError('Refusing to overwrite ' + helper)
            sftp.put(args.binary, helper)
            sftp.chmod(helper, 0o755)
            clients[name], transports[name] = sftp, transport
        try:
            for uid in [0, 104]:
                for mode in [0, 1]:
                    command = shlex.join(['/usr/bin/locally', helper, '0', str(mode), str(uid), helper, '/wki/wos-0' + helper, '0', '0'])
                    status, stdout, stderr = run(transports['wos-0'], command)
                    case = {'uid': uid, 'mode': mode, 'command': command, 'status': status, 'stdout': stdout, 'stderr': stderr}
                    result['cases'].append(case)
                    save()
                    print(stdout, flush=True)
                    lines = stdout.splitlines()
                    assert status == 0 and not stderr and len(lines) == 3, case
                    identities = set()
                    for stage, line in enumerate(lines):
                        assert line.startswith('PASS '), case
                        fields = dict(item.split('=', 1) for item in line.split()[1:])
                        assert int(fields['stage']) == stage and int(fields['uid']) == uid and int(fields['mode']) == mode
                        assert fields['host'] == ('wos-3' if mode and stage == 1 else 'wos-0')
                        assert int(fields['user']) + int(fields['system']) > 0
                        identities.add((fields['host'], int(fields['pid'])))
                    case['process_cleanup'] = []
                    for name, pid in sorted(identities):
                        deadline = time.monotonic() + 5
                        while True:
                            try:
                                clients[name].stat(f'/proc/{pid}/stat')
                            except FileNotFoundError:
                                case['process_cleanup'].append({'node': name, 'pid': pid, 'absent': True})
                                save()
                                break
                            if time.monotonic() >= deadline:
                                case['process_cleanup'].append({'node': name, 'pid': pid, 'absent': False})
                                save()
                                raise AssertionError('Owned process still visible: ' + str((name, pid)))
                            time.sleep(0.05)
        finally:
            for name, sftp in clients.items():
                sftp.remove(helper)
                result['removed'].append(name)
                save()


if __name__ == '__main__':
    main()
