#!/usr/bin/env python3
"""Validate PROCFS open identity, flags, FD duplication and denial cleanup."""
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
    path = "/tmp/goal19-procfs-open-probe-" + str(time.time_ns())
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.put(args.binary, path)
            sftp.chmod(path, 0o755)
            try:
                status, output, error = run(transport, shlex.quote(path))
                print(output, flush=True)
                if status or error or output.count("PASS profile=") != 16 or "PASS: 4560 native PROCFS open checks; cleanup=1" not in output:
                    raise RuntimeError(f"procfs open: status={status}, stderr={error!r}")
            finally:
                sftp.remove(path)
            print("fixture cleanup completed", flush=True)


if __name__ == "__main__":
    main()
