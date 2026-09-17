#!/usr/bin/env python3
"""Run functional mapped batch ownership, denial and complete-input checks in a disposable WOS cluster."""
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
    parser.add_argument("--base-root", choices=("/tmp", "/"), default="/tmp")
    parser.add_argument("--unsupported", action="store_true")
    args = parser.parse_args()
    suffix = str(time.time_ns())
    binary = "/tmp/goal19-batch-client-" + suffix
    base = args.base_root.rstrip("/") + "/goal19-batch-owner-" + suffix
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.get_channel().settimeout(30)
            sftp.put(args.binary, binary)
            sftp.chmod(binary, 0o755)

            def command(parts, expected):
                status, output, error = run(transport, shlex.join(parts))
                print(output, flush=True)
                if status or error or expected not in output:
                    raise RuntimeError(f"fixture: status={status}, stderr={error!r}")

            def owner(operation):
                command(["/usr/bin/on", "wos-3", "/wki/wos-0" + binary, operation, base], "PASS " + operation)

            try:
                owner("setup")
                if args.unsupported:
                    command([binary, "unsupported", "/wki/wos-3" + base], "PASS unsupported operation=2")
                else:
                    command([binary, "check", "/wki/wos-3" + base], "PASS batch checks=576")
                    command([binary, "bounds", "/wki/wos-3" + base], "PASS bounds checks=67")
                    print("PASS: 643 mapped batch checks", flush=True)
            finally:
                try:
                    owner("cleanup")
                finally:
                    sftp.remove(binary)
                print("fixture cleanup completed", flush=True)


if __name__ == "__main__":
    main()
