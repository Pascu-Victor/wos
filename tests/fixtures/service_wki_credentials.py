#!/usr/bin/env python3
"""Run after service_wki_identity.py has verified four-node export readiness."""
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
    parser.add_argument("--binary", required=True)
    parser.add_argument("--large-binary", required=True)
    args = parser.parse_args()
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            root = f"/tmp/goal19-wki-credentials-{time.time_ns()}"
            sftp.mkdir(root, 0o755)
            installed = []
            try:
                for source, size in ((args.binary, "small"), (args.large_binary, "large")):
                    path = f"{root}/{size}"
                    installed.append(path)
                    sftp.put(source, path)
                    sftp.chmod(path, 0o755)
                    for principal, uid in (("root", 0), ("user", 1000)):
                        for delivery in ("ordinary", "fragment"):
                            for target in ("wos-1", "wos-2", "wos-3"):
                                allowed = target == "wos-3" or (target == "wos-2" and uid == 1000)
                                expected = f"{target} uid={uid}/{uid}/{uid} gid={uid}/{uid}/{uid} groups=0 dumpable=1"
                                status, output, error = run(transport, f"{path} {target} {principal} {delivery}")
                                label = f"{size} {principal} {delivery} {target}"
                                print(f"{label}: status={status} stdout={output!r} stderr={error!r}", flush=True)
                                if error or status != (0 if allowed else 13):
                                    raise RuntimeError(f"unexpected credential result: {label}")
                                if output != (expected if allowed else "remote exec rejected: errno=13"):
                                    raise RuntimeError(f"credential mismatch: {label}")
                print("PASS: 24 credential cases across both ELF construction paths; refusals preserve caller state", flush=True)
            finally:
                for path in installed:
                    with contextlib.suppress(OSError):
                        sftp.remove(path)
                sftp.rmdir(root)


if __name__ == "__main__":
    main()
