#!/usr/bin/env python3
"""Validate initial WKI/HOST routes for common and foreign submitters on Goal19 VMs."""
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
    parser.add_argument("--component-capability", choices=("on", "off"), default="on")
    args = parser.parse_args()
    suffix = str(time.time_ns())
    binary = "/tmp/goal19-initial-routes-" + suffix
    base = "/goal19-initial-owner-" + suffix
    local = "/tmp/goal19-initial-local-" + suffix
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.get_channel().settimeout(30)
            sftp.put(args.binary, binary)
            sftp.chmod(binary, 0o755)

            def invoke(node, operation):
                command = [binary] if node == 0 else ["/usr/bin/on", "wos-3", "/wki/wos-0" + binary]
                return run(transport, shlex.join([*command, operation, base, local, args.component_capability]))

            try:
                for node in (0, 3):
                    status, output, error = invoke(node, "setup")
                    if status or error:
                        raise RuntimeError(f"setup node={node}: status={status}, output={output!r}, stderr={error!r}")
                for node in (0, 3):
                    status, output, error = invoke(node, "check")
                    print(output, flush=True)
                    if status or error or f"PASS initial routes node=wos-{node} checks=70" not in output:
                        raise RuntimeError(f"initial routes node={node}: status={status}, stderr={error!r}")
                print("PASS: 140 initial-route identity/component checks", flush=True)
            finally:
                failures = []
                for node in (3, 0):
                    status, output, error = invoke(node, "cleanup")
                    print(output, flush=True)
                    if status or error:
                        failures.append(f"node={node} status={status} stderr={error!r}")
                sftp.remove(binary)
                if failures:
                    raise RuntimeError("cleanup: " + "; ".join(failures))
                print("fixture cleanup completed", flush=True)


if __name__ == "__main__":
    main()
