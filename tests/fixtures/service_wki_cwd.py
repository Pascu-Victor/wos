#!/usr/bin/env python3
"""Run functional cwd identity and permission checks in a disposable WOS cluster."""
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
    parser.add_argument("--base-root", choices=("/tmp", "/"), default="/")
    parser.add_argument("--unsupported", action="store_true")
    parser.add_argument("--realpath", action="store_true")
    parser.add_argument("--aliases", action="store_true")
    parser.add_argument("--unsupported-realpath", action="store_true")
    args = parser.parse_args()
    suffix = str(time.time_ns())
    binary = "/tmp/goal19-cwd-client-" + suffix
    base = args.base_root.rstrip("/") + "/goal19-cwd-owner-" + suffix
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
                mode = "unsupported-realpath" if args.unsupported_realpath else "unsupported" if args.unsupported else "alias-check" if args.aliases else "realpath-check" if args.realpath else "check"
                count = 6 if args.unsupported or args.unsupported_realpath else 29 if args.aliases else 30 if args.realpath else 25
                label = mode if mode.startswith("unsupported") else "check"
                command([binary, mode, "/wki/wos-3" + base], f"PASS {label} checks={count} failed=0")
            finally:
                try:
                    owner("cleanup")
                finally:
                    sftp.remove(binary)
                print("fixture cleanup completed", flush=True)


if __name__ == "__main__":
    main()
