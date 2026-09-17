#!/usr/bin/env python3
"""Qualify onward exec refusals and completion to the original WOS parent."""
import argparse
import contextlib
import json
from pathlib import Path
import re
import shlex

import paramiko
from service_ssh_broker_lifetime import connect
from service_wki_identity import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('host')
    parser.add_argument('--username', default='root')
    parser.add_argument('--binary', required=True)
    parser.add_argument('--payload', required=True)
    parser.add_argument('--interpreter', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    root = '/tmp/goal19-onward-refusal'
    rows = []
    (args.output / 'session.json').write_text(json.dumps({'directory': root}) + '\n')
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError('an authorized SSH agent key is required')
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.get_channel().settimeout(30)
            sftp.mkdir(root, 0o755)
            sftp.chmod(root, 0o755)
            for source, name in ((args.binary, 'helper'), (args.payload, 'payload'), (args.interpreter, 'ld.so')):
                sftp.put(source, root + '/' + name)
                sftp.chmod(root + '/' + name, 0o755)
            for mode, bits in (('deny', 0o644), ('accept', 0o755)):
                sftp.chmod(root + '/ld.so', bits)
                assert sftp.stat(root + '/ld.so').st_mode & 0o777 == bits
                command = shlex.join([root + '/helper', 'wos-3', 'wos-0', '/wki/wos-0' + root + '/payload', mode])
                status, output, error = run(transport, command)
                row = {'mode': mode, 'status': status, 'stdout': output, 'stderr': error}
                rows.append(row)
                (args.output / 'results.json').write_text(json.dumps(rows, indent=2) + '\n')
                print(mode, status, output, error, flush=True)
                cases = re.findall(r'^CASE .* timely=1 reaped=1 status=5888 passed=1$', output, re.M)
                receivers = re.findall(r'^RECEIVER host=(\S+) pid=(\d+) uid=(\d+) worker=(\d+)$', output, re.M)
                witness = r'^REFUSED .* errno=13 state=1 descriptors=1 passed=1$' if mode == 'deny' else r'^FINAL host=wos-0 .* passed=1$'
                if status or error or len(cases) != 4 or len(receivers) != 4 or any(x[0] != 'wos-3' for x in receivers) or len(re.findall(witness, output, re.M)) != 4 or 'RESULT cases=4 failed=0' not in output:
                    raise RuntimeError('onward exec/completion failed; preserve owned fixture for review')
                pids = {'wos-3': [x[1] for x in receivers], 'wos-0': re.findall(r'^FINAL host=wos-0 pid=(\d+)', output, re.M)}
                row['retirement'] = {}
                for host, ids in pids.items():
                    if not ids:
                        continue
                    script = 'test -d /proc/$$ || exit 3; ' + '; '.join('test ! -d /proc/' + pid + ' || exit 4' for pid in ids)
                    prefix = ['/usr/bin/locally'] if host == 'wos-0' else ['/usr/bin/on', host]
                    retired, out, err = run(transport, shlex.join([*prefix, '/usr/bin/sh', '-c', script]))
                    row['retirement'][host] = {'status': retired, 'stdout': out, 'stderr': err}
                    (args.output / 'results.json').write_text(json.dumps(rows, indent=2) + '\n')
                    if retired or out or err:
                        raise RuntimeError('owned receiver retirement requires review')
            for name in ('helper', 'payload', 'ld.so'):
                sftp.remove(root + '/' + name)
            sftp.rmdir(root)
    (args.output / 'summary.json').write_text(json.dumps({'cases': 8, 'failed': 0, 'cleanup_completed': True}, indent=2) + '\n')


if __name__ == '__main__':
    main()
