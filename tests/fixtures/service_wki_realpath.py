#!/usr/bin/env python3
"""Check mapped REALPATH identity, canonical output, confinement and refusal."""
import argparse
import contextlib
import re
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
    parser.add_argument("--filesystem", choices=("tmpfs", "xfs"), default="tmpfs")
    parser.add_argument("--realpath", choices=("on", "off"), default="on")
    args = parser.parse_args()
    suffix = str(time.time_ns())
    binary = "/tmp/goal19-realpath-" + suffix
    alias = binary + "-alias"
    base = ("/tmp/" if args.filesystem == "tmpfs" else "/") + "goal19-realpath-owner-" + suffix
    remote = "/wki/wos-3" + base
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.put(args.binary, binary)
            sftp.chmod(binary, 0o755)

            def owner(operation):
                status, output, error = run(transport, shlex.join([
                    "/usr/bin/on", "wos-3", "/wki/wos-0" + binary, operation, base]))
                print(output, flush=True)
                if status or error or "PASS " + operation not in output:
                    raise RuntimeError(f"{operation}: status={status}, stderr={error!r}")

            try:
                owner("setup")
                sftp.symlink(remote, alias)
                status, output, error = run(transport, shlex.join([binary, "check", remote, alias, args.realpath]))
                print(output, flush=True)
                profiles = re.findall(r"profile grouped=\d passed=(\d+) total=(\d+)", output)
                if status or error or "PASS remote REALPATH checks=384" not in output or profiles != [("192", "192")] * 2:
                    raise RuntimeError(f"REALPATH: status={status}, stderr={error!r}")
                print(f"PASS: 384 mapped REALPATH checks on {args.filesystem}", flush=True)
            finally:
                try:
                    owner("cleanup")
                finally:
                    for path in (alias, binary):
                        try:
                            sftp.remove(path)
                        except FileNotFoundError:
                            pass
                print("fixture cleanup completed", flush=True)


if __name__ == "__main__":
    main()
