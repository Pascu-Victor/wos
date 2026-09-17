#!/usr/bin/env python3
"""Verify admitted truncate grants on disposable local and remote TMPFS/XFS files."""
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
    args = parser.parse_args()
    suffix = str(time.time_ns())
    binary = "/tmp/goal19-truncate-client-" + suffix
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.get_channel().settimeout(30)
            sftp.put(args.binary, binary)
            sftp.chmod(binary, 0o755)
            try:
                for root in ("/tmp", "/", "/wki/wos-3/tmp", "/wki/wos-3"):
                    base = root.rstrip("/") + "/goal19-truncate-owner-" + suffix
                    sftp.mkdir(base, 0o777)
                    sftp.chmod(base, 0o777)
                    try:
                        status, output, error = run(transport, shlex.join([binary, base]))
                        print(root, output, flush=True)
                        if status or error or "PASS truncate checks=126 failed=0" not in output:
                            raise RuntimeError(f"truncate fixture: status={status}, stderr={error!r}")
                    finally:
                        sftp.rmdir(base)
            finally:
                sftp.remove(binary)
            print("PASS: 504 descriptor truncate checks; fixture cleanup completed", flush=True)


if __name__ == "__main__":
    main()
