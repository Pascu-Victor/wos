#!/usr/bin/env python3
"""Verify ordinary native unmount retires exact exports and preserves busy bindings."""
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
    base = "/tmp/g19-export-" + str(time.time_ns())[-12:]
    helper, unrelated = base + "-helper", base + "-other"
    remote, remote_other = "/wki/" + args.owner + base, "/wki/" + args.owner + unrelated
    checks = 0

    def check(okay, name):
        nonlocal checks
        print(f"{'PASS' if okay else 'FAIL'} {name}", flush=True)
        if not okay:
            raise RuntimeError(name)
        checks += 1

    with contextlib.closing(paramiko.Agent()) as agent, contextlib.ExitStack() as stack:
        key = agent.get_keys()[0]
        source = stack.enter_context(connect(args, key))
        owner = stack.enter_context(connect(SimpleNamespace(host=args.owner_host, username=args.username), key))
        source_files = stack.enter_context(paramiko.SFTPClient.from_transport(source))
        owner_files = stack.enter_context(paramiko.SFTPClient.from_transport(owner))
        status, hostname, error = run(owner, "/usr/bin/locally /usr/bin/uname -n")
        check(status == 0 and not error and hostname == args.owner, "owner identity")

        def content(files, path):
            with files.file(path, "rb") as f:
                return f.read()

        def write(path, value):
            with owner_files.file(path, "w") as f:
                f.write(value)
            owner_files.chmod(path, 0o644)

        def operation(name):
            status, output, error = run(owner, shlex.join(["/usr/bin/locally", helper, name, base]))
            print(output, flush=True)
            if error:
                raise RuntimeError(error)
            return status, output

        def device(transport, path):
            status, output, error = run(transport, shlex.join(["/usr/bin/locally", helper, "stat", path]))
            if status or error or not output.startswith("dev="):
                raise RuntimeError(f"native stat failed: {status} {output!r} {error!r}")
            return int(output[4:])

        def mount_row(present):
            deadline = time.monotonic() + 30
            while True:
                status, output, error = run(source, "/usr/bin/locally /usr/bin/cat /proc/mounts")
                if status or error:
                    raise RuntimeError("mount table observation failed")
                rows = [line for line in output.splitlines() if len(line.split()) >= 3 and line.split()[1] == remote]
                if bool(rows) == present:
                    print(f"exact mount rows={rows}", flush=True)
                    return
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"exact mount presence expected={present} rows={rows}")
                time.sleep(0.1)

        def unmount_after_close():
            # Read-only remote CLOSE may be deferred after local fd removal.
            # Preserve every EBUSY result and bound its drain, rather than force it.
            deadline = time.monotonic() + 5
            while True:
                status, output = operation("unmount")
                if status == 0:
                    return
                if output != "operation=unmount rc=-1 errno=16" or time.monotonic() >= deadline:
                    raise RuntimeError("unmount did not commit after descriptor closure")
                time.sleep(0.1)

        owner_files.mkdir(base, 0o755)
        write(base + "/marker", "UNDER")
        write(unrelated, "KEEP")
        owner_files.put(args.binary, helper)
        owner_files.chmod(helper, 0o755)
        source_files.put(args.binary, helper)
        source_files.chmod(helper, 0o755)
        mounted = False
        held = keep = None
        print(f"fixture base={base} helper={helper} unrelated={unrelated}", flush=True)
        try:
            status, output, error = run(owner, shlex.join(["/usr/bin/locally", helper, "unmount", ""]))
            check(status == 1 and not error and output == "operation=unmount rc=-1 errno=36", "empty target preserves existing errno")
            status, _ = operation("mount")
            check(status == 0, "initial owner mount")
            mounted = True
            write(base + "/marker", "OLD")
            owner_dev = device(owner, base)
            mount_row(True)
            old_dev = device(source, remote)
            check(content(source_files, remote + "/marker") == b"OLD", "initial exact export content")
            held = source_files.file(remote + "/marker", "rb")
            keep = source_files.file(remote_other, "rb")
            other_dev = device(source, remote_other)
            status, output = operation("unmount")
            check(status == 1 and output == "operation=unmount rc=-1 errno=16", "held remote file keeps native unmount busy")
            mount_row(True)
            check(device(source, remote) == old_dev, "busy refusal preserves exact peer mount")
            check(held.read() == b"OLD", "busy refusal preserves issued remote file")
            check(keep.read() == b"KEEP", "busy refusal preserves unrelated issued file")
            held.close()
            held = None
            unmount_after_close()
            mounted = False
            mount_row(False)
            check(content(owner_files, base + "/marker") == b"UNDER", "underlying owner directory unchanged")
            check(device(source, remote_other) == other_dev, "commit preserves unrelated peer mount")
            keep.seek(0)
            check(keep.read() == b"KEEP", "commit preserves unrelated issued file")
            status, _ = operation("mount")
            check(status == 0, "replacement owner mount")
            mounted = True
            write(base + "/marker", "NEW")
            check(device(owner, base) != owner_dev, "replacement native identity differs")
            mount_row(True)
            check(device(source, remote) != old_dev, "replacement peer mount has fresh identity")
            check(content(source_files, remote + "/marker") == b"NEW", "replacement export content")
            unmount_after_close()
            mounted = False
            mount_row(False)
            check(content(owner_files, base + "/marker") == b"UNDER", "replacement retirement preserves underlying content")
        finally:
            if held is not None:
                held.close()
            if keep is not None:
                keep.close()
            if mounted:
                unmount_after_close()
                mount_row(False)
            owner_files.remove(base + "/marker")
            owner_files.rmdir(base)
            owner_files.remove(unrelated)
            owner_files.remove(helper)
            source_files.remove(helper)
        for files in (source_files, owner_files):
            check(not any(name.startswith(base.rsplit("/", 1)[1]) for name in files.listdir("/tmp")), "owned fixture paths removed")
        for transport in (source, owner):
            status, _, error = run(transport, "/usr/bin/sync")
            check(status == 0 and not error, "node synced")
        print(f"EXPORT_UNMOUNT checks={checks} failures=0 cleanup=1", flush=True)


if __name__ == "__main__":
    main()
