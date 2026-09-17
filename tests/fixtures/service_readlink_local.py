#!/usr/bin/env python3
"""Exercise local TMPFS/XFS retained READLINK using service_wki_readlink.cpp."""
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
    parser.add_argument("--xfs-long-link", help="pre-seeded data-block XFS link containing 400 x characters")
    args = parser.parse_args()
    suffix = str(time.time_ns())
    binary = f"/tmp/goal19-readlink-local-{suffix}-helper"
    files, directories = [], []
    count = 0
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.get_channel().settimeout(30)

            def check(profile, operation, path, error=0, target="file", size=512, local_route=False):
                nonlocal count
                arguments = [binary, profile, operation, path, str(error), target, str(size)]
                if local_route:
                    arguments.append("--local-devfs-route")
                command = shlex.join(arguments)
                status, output, stderr = run(transport, command)
                print(f"{path}: {output}", flush=True)
                if status or stderr or not output.startswith("PASS "):
                    raise RuntimeError(f"local READLINK failed: status={status}, stderr={stderr!r}")
                count += 1

            try:
                sftp.put(args.binary, binary)
                files.append(binary)
                sftp.chmod(binary, 0o755)
                for root in ("/tmp", ""):
                    base = f"{root}/goal19-readlink-local-{suffix}"
                    long_target = "x" * (400 if root else 200)
                    for directory, mode in ((base, 0o777), (base + "/private", 0o710), (base + "/closed", 0o700)):
                        sftp.mkdir(directory, mode)
                        directories.append(directory)
                        sftp.chmod(directory, mode)
                    sftp.chown(base + "/private", 1000, 2000)
                    path = base + "/file"
                    with sftp.open(path, "w") as output:
                        files.append(path)
                        output.write("regular file")
                    sftp.chmod(path, 0)
                    for link, target in (("link", "file"), ("directory", "."), ("private/link", "../file"),
                                         ("closed/link", "../file"), ("dangling", "absent"), ("absolute", "/dev/null"), ("long", long_target)):
                        path = base + "/" + link
                        sftp.symlink(target, path)
                        files.append(path)
                    for profile in ("fullroot", "root", "inert", "group", "other", "service"):
                        for operation in ("readlink", "raw"):
                            check(profile, operation, base + "/link")
                            check(profile, operation, base + "/link/", 20)
                            check(profile, operation, base + "/directory/", 22)
                            check(profile, operation, base + "/private/link", 13 if profile in ("root", "other") else 0, "../file")
                            check(profile, operation, base + "/closed/link", 0 if profile in ("fullroot", "root") else 13, "../file")
                        check(profile, "at", base + "/link", size=2)
                    for profile in ("inert", "group"):
                        check(profile, "readlink", base + "/link", size=1)
                        check(profile, "readlink", base + "/link", size=1024)
                        check(profile, "readlink", base + "/link", 22, size=0)
                        check(profile, "readlink", base + "/file", 22)
                        check(profile, "readlink", base + "/missing", 2)
                        check(profile, "readlink", base + "/dangling", target="absent")
                        check(profile, "readlink", base + "/absolute", target="/dev/null")
                        for size in (1, 2, len(long_target), 512, 1024):
                            check(profile, "readlink", base + "/long", target=long_target, size=size)
                if args.xfs_long_link:
                    for profile in ("inert", "group"):
                        for size in (1, 2, 400, 512, 1024):
                            check(profile, "readlink", args.xfs_long_link, target="x" * 400, size=size)
                for profile in ("fullroot", "inert", "group"):
                    check(profile, "readlink", "/dev/stdout", target="/proc/self/fd/1", size=2)
                    check(profile, "raw", "/dev/stdout", target="/proc/self/fd/1")
                    check(profile, "readlink", "/dev/null", 22)
                    check(profile, "readlink", "/dev/null/", 20)
                    check(profile, "readlink", "/dev/stdout", target="/proc/self/fd/1", size=2, local_route=True)
                    check(profile, "readlink", "/dev/null", 22, local_route=True)
                    check(profile, "readlink", "/dev/null/", 20, local_route=True)
                    check(profile, "readlink", "/dev/", 22, local_route=True)
                print(f"PASS: {count} local READLINK identity/type/output cases", flush=True)
            finally:
                errors = []
                for path in reversed(files):
                    try:
                        sftp.remove(path)
                    except OSError as error:
                        errors.append(f"{path}: {error}")
                for path in reversed(directories):
                    try:
                        sftp.rmdir(path)
                    except OSError as error:
                        errors.append(f"{path}: {error}")
                if errors:
                    raise RuntimeError("fixture cleanup failed: " + "; ".join(errors))


if __name__ == "__main__":
    main()
