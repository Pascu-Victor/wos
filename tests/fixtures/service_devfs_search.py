#!/usr/bin/env python3
"""Run local-submitter ancestor checks in a disposable WOS VM and restore /dev/pts."""
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
    binary = f"/tmp/goal19-devfs-search-{time.time_ns()}"
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.get_channel().settimeout(30)
            before = sftp.stat("/dev/pts").st_mode & 0o7777
            installed = False
            try:
                sftp.put(args.binary, binary)
                installed = True
                sftp.chmod(binary, 0o755)
                status, output, error = run(transport, shlex.quote(binary))
                print(output, flush=True)
                if status or error or output.count("PASS rules=") != 16 or "PASS: 16" not in output:
                    raise RuntimeError(f"ancestor-search fixture failed: status={status}, stderr={error!r}")
            finally:
                sftp.chmod("/dev/pts", before)
                if installed:
                    sftp.remove(binary)
                after = sftp.stat("/dev/pts").st_mode & 0o7777
                print(f"parent mode restored: {after:o}", flush=True)
                if after != before:
                    raise RuntimeError("parent mode restoration failed")


if __name__ == "__main__":
    main()
