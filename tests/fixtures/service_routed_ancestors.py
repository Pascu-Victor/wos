#!/usr/bin/env python3
"""Check receiver ancestor denial after privileged cache warmup on Goal19 VMs."""
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
    parser.add_argument("--dot-ancestor", action="store_true")
    parser.add_argument("--component-capability", choices=("on", "off"), default="on")
    args = parser.parse_args()
    if args.component_capability == "off" and not args.dot_ancestor:
        parser.error("--component-capability off requires --dot-ancestor")
    suffix = str(time.time_ns())
    binary = "/tmp/goal19-routed-ancestors-" + suffix
    base = "/tmp/goal19-routed-search-" + suffix
    origin = "/goal19-route-origin-" + suffix
    private = "/goal19-route-private-" + suffix
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.get_channel().settimeout(30)
            sftp.put(args.binary, binary)
            try:
                sftp.chmod(binary, 0o755)
                with sftp.open(origin, "w") as output:
                    output.write("origin")
                sftp.chmod(origin, 0o644)
                target = origin
                mode = []
                if args.dot_ancestor:
                    sftp.mkdir(private, 0o700)
                    sftp.chmod(private, 0o700)
                    target = private + "/../" + origin[1:]
                    mode = ["dot" if args.component_capability == "on" else "capoff"]
                status, output, error = run(transport, shlex.join([
                    "/usr/bin/on", "wos-3", "/wki/wos-0" + binary, base, target, *mode,
                ]))
                print(output, flush=True)
                count = 25 if args.dot_ancestor else 24
                if status or error or f"passed={count} total={count}" not in output or "cleanup=1" not in output:
                    raise RuntimeError(f"ancestor regression failed: status={status}, stderr={error!r}")
            finally:
                sftp.remove(binary)
                try:
                    sftp.remove(origin)
                except FileNotFoundError:
                    pass
                if args.dot_ancestor:
                    try:
                        sftp.rmdir(private)
                    except FileNotFoundError:
                        pass
                for path in (base + "/private/link", base + "/private/host", base + "/host", base + "/self", base + "/public"):
                    try:
                        sftp.remove("/wki/wos-3" + path)
                    except FileNotFoundError:
                        pass
                for path in (base + "/private", base):
                    try:
                        sftp.rmdir("/wki/wos-3" + path)
                    except FileNotFoundError:
                        pass
                print("fixture cleanup completed", flush=True)


if __name__ == "__main__":
    main()
