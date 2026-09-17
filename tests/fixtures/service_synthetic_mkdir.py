#!/usr/bin/env python3
"""Run synthetic mkdir denial checks on a disposable WOS VM with SSH ready."""
import argparse
import contextlib

import paramiko

from service_ssh_broker_lifetime import connect
from service_wki_identity import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("--username", default="root")
    parser.add_argument("--binary", required=True)
    args = parser.parse_args()
    path = "/tmp/goal19-synthetic-mkdir"
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.get_channel().settimeout(30)
            try:
                sftp.stat(path)
            except OSError as error:
                if error.errno != 2:
                    raise
            else:
                raise RuntimeError(f"fixture path already exists: {path}")
            try:
                sftp.put(args.binary, path)
                sftp.chmod(path, 0o755)
                status, output, error = run(transport, path)
                print(output, flush=True)
                if status or error or not output.endswith("PASS: 24 synthetic mkdir denials with absent paths preserved"):
                    raise RuntimeError(f"native denial failure: status={status}, stderr={error!r}")
            finally:
                sftp.remove(path)


if __name__ == "__main__":
    main()
