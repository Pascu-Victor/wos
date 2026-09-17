#!/usr/bin/env python3
"""Check remote alias export selection and preservation of denied state."""
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
    args = parser.parse_args()
    suffix = str(time.time_ns())
    binary = "/tmp/goal19-alias-export-" + suffix
    alias = binary + "-alias"
    base = "/tmp/goal19-alias-export-" + suffix
    outside = "/goal19-alias-export-outside-" + suffix
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
                    "/usr/bin/on", "wos-3", "/wki/wos-0" + binary, operation, base, outside]))
                print(output, flush=True)
                if status or error or "PASS " + operation not in output:
                    raise RuntimeError(f"{operation}: status={status}, stderr={error!r}")

            try:
                owner("setup")
                sftp.symlink(remote, alias)
                status, output, error = run(transport, shlex.join([binary, "check", remote, alias, outside]))
                print(output, flush=True)
                profiles = re.findall(r"profile grouped=\d passed=(\d+) total=(\d+)", output)
                if status or error or "PASS alias export checks=208" not in output or profiles != [("104", "104")] * 2:
                    raise RuntimeError(f"alias export: status={status}, stderr={error!r}")
                owner("verify")
                print("PASS: 208 alias export checks; denied create/truncate left owner state unchanged", flush=True)
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
