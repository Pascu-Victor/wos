#!/usr/bin/env python3
"""Run after service_wki_identity.py: verify mapped remote PT_INTERP read/execute admission."""
import argparse
import contextlib

import paramiko

from service_ssh_broker_lifetime import connect
from service_wki_identity import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("--username", default="root")
    parser.add_argument("--target", default="wos-3")
    parser.add_argument("--launcher", required=True, help="service_wki_credentials.cpp with the normal interpreter")
    parser.add_argument("--binary", required=True,
                        help="service_wki_credentials.cpp linked with /wki/wos-0/tmp/goal19-wki-interp/ld.so")
    parser.add_argument("--interpreter", required=True)
    parser.add_argument("--read-denial-errno", type=int, choices=(5, 12), default=5,
                        help="existing read-denial errno: 5 for buffer construction, 12 for file-backed construction")
    args = parser.parse_args()
    root = "/tmp/goal19-wki-interp"
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.mkdir(root, 0o755)
            try:
                sftp.put(args.launcher, f"{root}/check")
                sftp.put(args.binary, f"{root}/payload")
                sftp.chmod(f"{root}/payload", 0o755)
                sftp.chmod(f"{root}/check", 0o755)
                sftp.put(args.interpreter, f"{root}/ld.so")
                for delivery in ("ordinary", "fragment"):
                    for mode, principal, expected_status in ((0o700, "root", 0), (0o700, "user", args.read_denial_errno),
                                                              (0o644, "root", 13), (0o744, "user", 13), (0o755, "user", 0)):
                        sftp.chmod(f"{root}/ld.so", mode)
                        command = f"{root}/check {args.target} {principal} {delivery} {root}/payload"
                        status, output, error = run(transport, command)
                        print(f"mode={mode:o} {principal} {delivery}: status={status} stdout={output!r} stderr={error!r}", flush=True)
                        uid = 0 if principal == "root" else 1000
                        # A readable plain exec resets dumpability even after an ID drop.
                        expected_output = (f"{args.target} uid={uid}/{uid}/{uid} gid={uid}/{uid}/{uid} groups=0 dumpable=1"
                                           if expected_status == 0 else f"remote exec rejected: errno={expected_status}")
                        if status != expected_status or output != expected_output or error:
                            raise RuntimeError("mapped remote interpreter admission failed")
                print("PASS: executable warmup/control, private read denial and root/user execute denial across both delivery modes", flush=True)
            finally:
                for name in ("check", "payload", "ld.so"):
                    with contextlib.suppress(OSError):
                        sftp.remove(f"{root}/{name}")
                sftp.rmdir(root)


if __name__ == "__main__":
    main()
