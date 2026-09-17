#!/usr/bin/env python3
"""Run after service_wki_identity.py: verify mapped receiver ELF source reads.

Mode 0711 permits execution but denies UID 1000 the image read needed for
construction. A refusal may occur during the sender's retained read admission
or the receiver's mapped read; this fixture asserts the end-to-end result.
"""
import argparse
import contextlib
import time

import paramiko

from service_ssh_broker_lifetime import connect
from service_wki_identity import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("--username", default="root")
    parser.add_argument("--target", default="wos-3")
    parser.add_argument("--launcher", required=True)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--large-binary", required=True)
    args = parser.parse_args()
    root = f"/tmp/goal19-wki-source-{time.time_ns()}"
    launcher = f"{root}/launcher"
    remote_root = f"/wki/{args.target}{root}"
    remote = f"{remote_root}/payload"
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            installed = []
            created_roots = []
            try:
                for directory in (root, remote_root):
                    sftp.mkdir(directory, 0o755)
                    created_roots.append(directory)
                installed.append(launcher)
                sftp.put(args.launcher, launcher)
                sftp.chmod(launcher, 0o755)
                for source, size in ((args.binary, "small"), (args.large_binary, "large")):
                    path = f"{remote}-{size}"
                    installed.append(path)
                    sftp.put(source, path)
                    for delivery in ("ordinary", "fragment"):
                        for mode, principal, expected_status in ((0o711, "root", 0), (0o711, "user", 13), (0o755, "user", 0)):
                            sftp.chmod(path, mode)
                            status, output, error = run(transport, f"{launcher} {args.target} {principal} {delivery} {path}")
                            print(f"{size} mode={mode:o} {principal} {delivery}: status={status} stdout={output!r} stderr={error!r}", flush=True)
                            uid = 0 if principal == "root" else 1000
                            expected = (f"{args.target} uid={uid}/{uid}/{uid} gid={uid}/{uid}/{uid} groups=0 dumpable=1"
                                        if expected_status == 0 else "remote exec rejected: errno=13")
                            if status != expected_status or output != expected or error:
                                raise RuntimeError("mapped receiver ELF source admission failed")
                print("PASS: 12 private-source warmup, denial and readable controls; refusals preserve caller state", flush=True)
            finally:
                for path in reversed(installed):
                    with contextlib.suppress(OSError):
                        sftp.remove(path)
                for directory in reversed(created_roots):
                    sftp.rmdir(directory)


if __name__ == "__main__":
    main()
