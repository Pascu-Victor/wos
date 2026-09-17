#!/usr/bin/env python3
"""Retained-FD metadata on disposable WOS TMPFS exports; optional capoff receiver3."""
import argparse
import contextlib
import shlex
import time

import paramiko

from service_ssh_broker_lifetime import connect
from service_wki_identity import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("--username", default="root")
    parser.add_argument("--binary", required=True)
    parser.add_argument("--capability-off", action="store_true")
    args = parser.parse_args()
    helper = "/tmp/goal19-wki-fdstat"
    suffix = f"goal19-fdstat-{time.time_ns()}"
    total = 0
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.get_channel().settimeout(30)
            try:
                sftp.stat(helper)
            except OSError as error:
                if error.errno != 2:
                    raise
            else:
                raise RuntimeError(f"fixture already exists: {helper}")
            workspaces = []
            installed = False
            try:
                sftp.put(args.binary, helper)
                installed = True
                sftp.chmod(helper, 0o755)
                nodes = (3,) if args.capability_off else (2, 3)
                for node in nodes:
                    scenarios = ("owned", "writeonly", "drop") if node == 3 else ("owned", "writeonly")
                    for scenario in scenarios:
                        workspace = f"/wki/wos-{node}/tmp/{suffix}-{scenario}"
                        sftp.mkdir(workspace, 0o777)
                        workspaces.append(workspace)
                        sftp.chmod(workspace, 0o777)
                        command = shlex.join([helper, workspace, scenario, "off" if args.capability_off else "on"])
                        status, output, error = run(transport, command)
                        print(f"node={node}: status={status} stdout={output!r} stderr={error!r}", flush=True)
                        if status or error or not output.startswith("PASS "):
                            raise RuntimeError(f"descriptor metadata scenario failed: {command}")
                        # RMDIR proves helper cleanup without borrowing root identity for remote STAT.
                        sftp.rmdir(workspace)
                        workspaces.remove(workspace)
                        total += 1
                print(f"PASS: {total} retained-descriptor metadata scenarios", flush=True)
            finally:
                for workspace in reversed(workspaces):
                    # Only exact files in our newly created workspace belong to this fixture.
                    for name in ("original", "moved"):
                        try:
                            sftp.remove(f"{workspace}/{name}")
                        except OSError as error:
                            if error.errno != 2:
                                raise
                    sftp.rmdir(workspace)
                if installed:
                    sftp.remove(helper)


if __name__ == "__main__":
    main()
