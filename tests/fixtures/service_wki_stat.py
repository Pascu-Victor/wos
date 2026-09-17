#!/usr/bin/env python3
"""Scalar/batch owner-search checks on the disposable Goal19 four-node cluster."""
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
    helper = "/tmp/goal19-wki-stat"
    suffix = f"goal19-stat-{time.time_ns()}"
    files, directories = [], []
    count = 0
    installed = False
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

            def check(profile, operation, path, expected=0, items=()):
                nonlocal count
                arguments = [helper, profile, operation, path, str(expected)]
                for item, status in items:
                    arguments.extend([item, str(status)])
                command = shlex.join(arguments)
                status, output, error = run(transport, command)
                print(f"{path}: {output}", flush=True)
                if status or error or not output.startswith("PASS "):
                    raise RuntimeError(f"metadata failure: {command}: {status}, {output!r}, {error!r}")
                count += 1

            try:
                sftp.put(args.binary, helper)
                installed = True
                sftp.chmod(helper, 0o755)
                check("inert", "directory", "/wki/wos-1/tmp", 13)
                check("inert", "batch", "denied-peer", 13, [("/wki/wos-1/tmp/missing", 2)])
                for node in (2, 3):
                    prefix = f"/wki/wos-{node}/tmp"
                    if node == 3 and args.capability_off:
                        alias = f"/tmp/{suffix}-unsupported-alias"
                        sftp.symlink(prefix, alias)
                        files.append(alias)
                        for profile in ("fullroot", "inert", "group", "other", "service"):
                            for operation in ("directory", "link", "exists", "readable", "searchable"):
                                check(profile, operation, prefix, 95)
                            check(profile, "batch", "unsupported-peer", 95, [(f"{prefix}/missing", 2)])
                            for operation in ("directory", "link", "exists", "readable", "searchable"):
                                check(profile, operation, f"{alias}/missing", 95)
                            check(profile, "batch", "unsupported-local-alias", 95, [(f"{alias}/missing", 2)])
                        continue
                    base = f"{prefix}/{suffix}"
                    private, closed = f"{base}/private", f"{base}/closed"
                    for path, mode in ((base, 0o777), (closed, 0o700)):
                        sftp.mkdir(path, mode)
                        directories.append(path)
                        sftp.chmod(path, mode)
                    # CHOWN is not implemented remotely. Stage with an actual local
                    # mkdir on the owner under the mapped inert identity instead.
                    command = shlex.join([helper, "--stage-owner", f"wos-{node}", f"/tmp/{suffix}/private", "0710"])
                    status, output, error = run(transport, command)
                    print(f"owner staging node={node}: status={status} {output!r} {error!r}", flush=True)
                    if status or error or not output.startswith("PASS "):
                        raise RuntimeError("owner-local directory staging failed")
                    directories.append(private)
                    owned = f"{private}/file"
                    check("inert", "create", owned)
                    files.append(owned)
                    sftp.chmod(owned, 0)
                    link, escape, dangling, hidden = (f"{base}/{x}" for x in ("link", "escape", "dangling", "closed/link"))
                    for target, path in (("private/file", link), ("/rootfs/dev/null", escape), ("absent", dangling), ("../private/file", hidden)):
                        sftp.symlink(target, path)
                        files.append(path)
                    hidden_directory = f"{closed}/directory"
                    sftp.symlink("..", hidden_directory)
                    files.append(hidden_directory)
                    # A source-local symlink must preserve the remote remainder
                    # until the mapped owner performs its ancestor search.
                    alias = f"/tmp/{suffix}-alias-{node}"
                    sftp.symlink(base, alias)
                    files.append(alias)
                    if node == 3:
                        check("fullroot", "stat", f"{alias}/closed/link")
                        check("fullroot", "open-directory", f"{alias}/closed/directory")
                    for profile in ("inert", "group"):
                        check(profile, "stat", f"{alias}/closed/link", 13)
                        check(profile, "link", f"{alias}/closed/link", 13)
                        check(profile, "exists", f"{alias}/closed/link", 13)
                        check(profile, "open-directory", f"{alias}/closed/directory", 13)
                        check(profile, "open-directory", f"{alias}/")
                        check(profile, "stat", f"{alias}/link")
                        check(profile, "link", f"{alias}/link")
                        check(profile, "stat", f"{alias}/link/", 20)
                        check(profile, "stat", f"{alias}/dangling", 2)
                        check(profile, "batch", "local-alias", 0,
                              [(f"{alias}/private/file", 0), (f"{alias}/closed/link", 13)])
                    # Privileged warmup must not cache away a later search denial.
                    if node == 3:
                        check("fullroot", "stat", hidden)
                        check("fullroot", "stat", owned)
                    for profile in ("inert", "group", "other", "root", "service"):
                        expected = 13 if profile in ("other", "root") or (node == 2 and profile == "service") else 0
                        check(profile, "stat", owned, expected)
                        check(profile, "stat", link, expected)
                        check(profile, "exists", owned, expected)
                        check(profile, "searchable", private, expected)
                        check(profile, "readable", owned, 13)
                    for profile in ("inert", "group"):
                        check(profile, "link", link)
                        check(profile, "stat", f"{link}/", 20)
                        check(profile, "link", f"{link}/", 20)
                        check(profile, "stat", hidden, 13)
                        check(profile, "link", hidden, 13)
                        check(profile, "stat", escape, 1)
                        check(profile, "link", escape)
                        check(profile, "stat", dangling, 2)
                        check(profile, "link", dangling)
                        check(profile, "nofollow-exists", dangling)
                        check(profile, "batch", "mixed-items", 0, [(owned, 0), (hidden, 13), (dangling, 2), (link, 0)])
                    check("group", "batch", "chunk-boundary", 0, [(owned, 0)] * 64)
                    check("other", "batch", "all-denied", 0, [(owned, 13), (hidden, 13)])
                    if node == 2:
                        check("fullroot", "stat", owned, 13)
                        check("root", "batch", "root-untrusted", 13, [(owned, 0)])
                print(f"PASS: {count} scalar/batch pathname metadata cases", flush=True)
            finally:
                for path in reversed(files):
                    sftp.remove(path)
                for path in reversed(directories):
                    sftp.rmdir(path)
                if installed:
                    sftp.remove(helper)


if __name__ == "__main__":
    main()
