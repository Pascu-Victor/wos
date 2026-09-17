#!/usr/bin/env python3
"""Verify remote parent cover and native retained-directory authority in owned fixtures."""
import argparse
import contextlib
import shlex
import time
from types import SimpleNamespace

import paramiko

from service_ssh_broker_lifetime import connect
from service_wki_identity import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("--owner-host", required=True)
    parser.add_argument("--owner", default="wos-3")
    parser.add_argument("--username", default="root")
    parser.add_argument("--binary", required=True)
    args = parser.parse_args()
    suffix = str(time.time_ns())
    child = "goal19-mixed-child-" + suffix
    base = "/tmp/goal19-mixed-parent-" + suffix
    binary = "/tmp/goal19-mixed-helper-" + suffix
    owner = "/tmp/" + child
    print(f"fixture binary={binary} source={base} owner={owner}", flush=True)
    with contextlib.closing(paramiko.Agent()) as agent, contextlib.ExitStack() as stack:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        source_transport = stack.enter_context(connect(args, keys[0]))
        owner_transport = stack.enter_context(connect(SimpleNamespace(host=args.owner_host, username=args.username), keys[0]))
        source_files = stack.enter_context(paramiko.SFTPClient.from_transport(source_transport))
        owner_files = stack.enter_context(paramiko.SFTPClient.from_transport(owner_transport))
        status, output, error = run(owner_transport, "/usr/bin/locally /usr/bin/uname -n")
        if status or error or output != args.owner:
            raise RuntimeError("owner hostname does not match configured peer")
        owner_files.mkdir(owner, 0o755)
        owner_files.mkdir(owner + "-new", 0o755)
        with owner_files.file(owner + "/marker", "w") as marker:
            marker.write("REMOTE")
        owner_files.chmod(owner + "/marker", 0o644)
        source_files.put(args.binary, binary)
        source_files.chmod(binary, 0o755)
        try:
            status, output, error = run(source_transport, shlex.join([
                "/usr/bin/locally", binary, base, child, "wki://" + args.owner + "//tmp"]))
            print(output, flush=True)
            if status or error or output.count("PASS ") != 24 or "MOUNT_COVER failures=0 cleanup=1" not in output:
                raise RuntimeError(f"mount cover status={status} stderr={error!r}")
            with owner_files.file(owner + "/marker", "r") as marker:
                if marker.read() != b"REMOTE":
                    raise RuntimeError("owner marker changed")
            if owner_files.stat(owner + "/marker").st_mode & 0o7777 != 0o644 or owner_files.listdir(owner) != ["marker"]:
                raise RuntimeError("owner metadata or directory changed")
            if owner_files.listdir(owner + "-new"):
                raise RuntimeError("fresh owner mount target changed")
            try:
                source_files.stat(base)
            except FileNotFoundError:
                pass
            else:
                raise RuntimeError("source fixture directory remains")
        finally:
            source_files.remove(binary)
            owner_files.remove(owner + "/marker")
            owner_files.rmdir(owner)
            owner_files.rmdir(owner + "-new")
        for transport in (source_transport, owner_transport):
            status, _, error = run(transport, "/usr/bin/sync")
            if status or error:
                raise RuntimeError("fixture sync failed")
        print("owner contents/mode unchanged; fixture cleanup completed", flush=True)


if __name__ == "__main__":
    main()
