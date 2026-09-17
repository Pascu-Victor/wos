#!/usr/bin/env python3
"""Mapped READLINK identity, ancestor, output and negotiation checks on Goal19 VMs."""
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
    parser.add_argument("--directory-paths-off", action="store_true", help="receiver wos-3 rejects descriptor-relative path selectors")
    args = parser.parse_args()
    helper = "/tmp/goal19-wki-readlink"
    suffix = f"goal19-readlink-{time.time_ns()}"
    profiles = ("fullroot", "inert", "group", "other", "root", "service")
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
                raise RuntimeError(f"fixture already exists: {helper}")

            def check(profile, path, expected=0, target="", size=512, operation="readlink"):
                nonlocal count
                command = shlex.join([helper, profile, operation, path, str(expected), target, str(size)])
                status, output, error = run(transport, command)
                print(f"{path}: {output}", flush=True)
                if status or error or not output.startswith("PASS "):
                    raise RuntimeError(f"READLINK failure: {command}: {status}, {output!r}, {error!r}")
                count += 1

            def mkdir(path, mode):
                sftp.mkdir(path, mode)
                directories.append(path)
                sftp.chmod(path, mode)

            def link(target, path):
                sftp.symlink(target, path)
                files.append(path)

            def regular(path):
                check("inert", path, operation="create")
                files.append(path)
                sftp.chmod(path, 0)

            try:
                sftp.put(args.binary, helper)
                installed = True
                sftp.chmod(helper, 0o755)
                check("inert", "/wki/wos-1/tmp", 13)
                check("inert", "/wki/wos-1/tmp/missing", 13, operation="raw")
                for node in (2, 3):
                    prefix = f"/wki/wos-{node}/tmp"
                    if node == 3 and args.capability_off:
                        alias = f"/tmp/{suffix}-unsupported"
                        link(prefix, alias)
                        for profile in profiles:
                            for operation in ("readlink", "raw"):
                                for path in (prefix, f"{prefix}/missing", f"{alias}/missing"):
                                    check(profile, path, 95, operation=operation)
                            check(profile, f"{prefix}/missing", 95, operation="at")
                            check(profile, f"{alias}/missing", 22, size=0)
                        continue
                    base = f"{prefix}/{suffix}"
                    private, closed = f"{base}/private", f"{base}/closed"
                    mkdir(base, 0o777)
                    mkdir(closed, 0o700)
                    command = shlex.join([helper, "--stage-owner", f"wos-{node}", f"/tmp/{suffix}/private", "0710"])
                    status, output, error = run(transport, command)
                    print(f"owner staging node={node}: status={status} {output!r} {error!r}", flush=True)
                    if status or error or not output.startswith("PASS "):
                        raise RuntimeError("owner-local staging failed")
                    directories.append(private)
                    regular(f"{base}/file")
                    for target, path in (("file", f"{base}/link"), ("../file", f"{private}/link"),
                                         ("../file", f"{closed}/link"), ("absent", f"{base}/dangling"),
                                         (".", f"{base}/directory"), ("/rootfs/dev", f"{base}/escape")):
                        link(target, path)
                    alias = f"/tmp/{suffix}-alias-{node}"
                    link(base, alias)
                    if node == 3:
                        check("fullroot", f"{private}/link", target="../file")
                        check("fullroot", f"{alias}/closed/link", target="../file")
                    for profile in profiles:
                        policy_denied = node == 2 and profile in ("fullroot", "root", "service")
                        public_error = 13 if policy_denied else 0
                        private_error = 13 if policy_denied or profile in ("other", "root") else 0
                        check(profile, f"{base}/link", public_error, "file")
                        check(profile, f"{private}/link", private_error, "../file")
                        check(profile, f"{alias}/private/link", private_error, "../file", operation="raw")
                        check(profile, f"{alias}/closed/link", 0 if profile in ("fullroot", "root") and node == 3 else 13, "../file")
                    for profile in ("inert", "group"):
                        for size in (1, 2, 4, 512, 1024):
                            check(profile, f"{private}/link", target="../file", size=size)
                        directory_error = 95 if node == 3 and args.directory_paths_off else 0
                        check(profile, f"{base}/link", directory_error, target="file", operation="at")
                        check(profile, f"{alias}/link", directory_error, target="file", operation="at")
                        check(profile, f"{base}/link", 22, size=0)
                        check(profile, f"{base}/file", 22)
                        check(profile, f"{base}/missing", 2)
                        check(profile, f"{base}/dangling", target="absent")
                        check(profile, f"{base}/link/", 20)
                        check(profile, f"{base}/directory/", 22)
                        check(profile, f"{base}/escape", target="/rootfs/dev")
                        check(profile, f"{base}/escape/stdout", 1)
                        check(profile, prefix, 22)
                        check(profile, f"/wki/wos-{node}/dev/stdout", target="/proc/self/fd/1")
                        check(profile, f"/wki/wos-{node}/dev/null", 22)
                        check(profile, f"/wki/wos-{node}/proc/self", 95)
                        check(profile, f"/wki/wos-{node}/boot", 38)
                    # Exercise the same wire adapter on the owner XFS root export.
                    xfs = f"/wki/wos-{node}/{suffix}-xfs"
                    mkdir(xfs, 0o777)
                    mkdir(f"{xfs}/closed", 0o700)
                    link("literal", f"{xfs}/link")
                    link("../link", f"{xfs}/closed/link")
                    regular(f"{xfs}/file")
                    if node == 3:
                        check("fullroot", f"{xfs}/closed/link", target="../link")
                    for profile in ("inert", "group"):
                        check(profile, f"{xfs}/link", target="literal")
                        check(profile, f"{xfs}/closed/link", 13)
                        check(profile, f"{xfs}/file", 22)
                        check(profile, f"{xfs}/link", target="literal", size=2)
                print(f"PASS: {count} mapped READLINK identity/output cases", flush=True)
            finally:
                for path in reversed(files):
                    sftp.remove(path)
                for path in reversed(directories):
                    sftp.rmdir(path)
                if installed:
                    sftp.remove(helper)


if __name__ == "__main__":
    main()
