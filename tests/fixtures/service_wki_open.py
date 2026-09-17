#!/usr/bin/env python3
"""Verify ordinary OPEN identity on a disposable four-node WOS cluster.

Receiver 1 denies identity, 2 trusts nonroot same-domain, and 3 trusts root too.
The helper runs locally; every tested pathname names an owner through /wki.
Private 0777 workspaces permit caller-owned creation without changing WOS /tmp
(which is root-owned 0755). File data and mode assertions use ordinary OPEN.
Use --capability-off for a second boot with receiver 3's open capability off.
"""
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
    parser.add_argument("--root-prefix", default="/wki/wos-3", help="mount of receiver 3 root export")
    args = parser.parse_args()
    helper = "/tmp/goal19-wki-open"
    suffix = f"goal19-open-{time.time_ns()}"
    created = []
    workspaces = []
    count = 0
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
                raise RuntimeError(f"fixture path already exists: {helper}")

            def check(profile, operation, path, expected=0):
                nonlocal count
                command = shlex.join([helper, profile, operation, path, str(expected)])
                status, output, error = run(transport, command)
                print(f"{path}: {output}", flush=True)
                if status or error or not output.startswith("PASS "):
                    raise RuntimeError(f"ordinary open failure: {command}: {status}, {output!r}, {error!r}")
                count += 1

            try:
                sftp.put(args.binary, helper)
                sftp.chmod(helper, 0o755)
                if args.capability_off:
                    for profile in ("root", "inert", "service"):
                        for operation, path in (("read", "/tmp"), ("create", f"/tmp/{suffix}"), ("directory", "/tmp")):
                            check(profile, operation, f"{args.root_prefix}{path}", 95)
                    try:
                        sftp.stat(f"{args.root_prefix}/tmp/{suffix}")
                    except OSError as error:
                        if error.errno != 2:
                            raise
                    else:
                        raise RuntimeError("capability refusal created a file")
                else:
                    for profile in ("root", "inert", "service", "group", "other"):
                        check(profile, "directory", "/wki/wos-1/tmp", 13)
                        check(profile, "directory", "/wki/wos-2/tmp", 13 if profile in ("root", "service") else 0)
                        check(profile, "directory", f"{args.root_prefix}/tmp")
                    for node in (2, 3):
                        prefix = args.root_prefix if node == 3 else f"/wki/wos-{node}"
                        workspace = f"{prefix}/tmp/{suffix}"
                        sftp.mkdir(workspace, 0o777)
                        workspaces.append(workspace)
                        sftp.chmod(workspace, 0o777)
                        owned = f"{workspace}/owned"
                        check("inert", "create", owned)
                        created.append(owned)
                        check("inert", "preserved", owned)
                        check("inert", "create", owned, 17)
                        check("other", "read", owned, 13)
                        check("other", "truncate", owned, 13)
                        check("inert", "preserved", owned)
                        check("group", "preserved", owned)
                        check("group", "write", owned, 13)
                        check("group", "truncate", owned, 13)
                        closed = f"{workspace}/closed"
                        sftp.mkdir(closed, 0o700)
                        workspaces.append(closed)
                        sftp.chmod(closed, 0o700)
                        link = f"{closed}/link"
                        sftp.symlink("../owned", link)
                        created.append(link)
                        check("group", "read", link, 13)
                        check("group", "truncate", link, 13)
                        sftp.chmod(closed, 0o755)
                        check("group", "preserved", link)
                        check("group", "nofollow", link, 40)
                        check("inert", "preserved", owned)
                        check("inert", "write", owned)
                        check("inert", "truncate", owned)
                        check("inert", "empty", owned)
                        check("inert", "directory", f"{prefix}/tmp")
                        check("inert", "read", f"{owned}-missing", 2)
                        check("inert", "create", f"{prefix}/tmp/{suffix}-denied", 13)
                        check("inert", "read", f"{prefix}/tmp/{suffix}-denied", 2)
                    root_owned = f"{args.root_prefix}/tmp/{suffix}-root"
                    check("root", "create", root_owned)
                    created.append(root_owned)
                    check("root", "preserved", root_owned)
                    check("inert", "read", root_owned, 13)
                    check("inert", "truncate", root_owned, 13)
                    check("root", "preserved", root_owned)
                print(f"PASS: {count} ordinary remote OPEN cases; denied creates left paths absent", flush=True)
            finally:
                for path in reversed(created):
                    sftp.remove(path)
                for path in reversed(workspaces):
                    sftp.rmdir(path)
                sftp.remove(helper)


if __name__ == "__main__":
    main()
