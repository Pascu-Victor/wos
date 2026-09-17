#!/usr/bin/env python3
"""Check default service credential snapshots and state ownership on two disposable WOS nodes."""

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
    parser.add_argument("--peer", default="wos-3")
    args = parser.parse_args()
    path = "/tmp/goal19-service-runtime-" + str(time.time_ns())
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            try:
                sftp.put(args.binary, path)
                sftp.chmod(path, 0o755)
                commands = (
                    ("wos-0", "/usr/bin/locally " + shlex.quote(path) + " lan wos-0"),
                    (args.peer, "/usr/bin/on " + shlex.quote(args.peer) + " " + shlex.quote("/wki/wos-0" + path) +
                     " wki " + shlex.quote(args.peer)),
                )
                failed = []
                for node, command in commands:
                    status, output, error = run(transport, command)
                    print(f"NODE {node}: exit={status}\n{output}", flush=True)
                    if status or error or "failed" not in output or "PASS:" not in output or "FAIL" in output:
                        failed.append(f"{node}: exit={status}, stderr={error!r}")
                if failed:
                    raise RuntimeError("; ".join(failed))
            finally:
                sftp.remove(path)
                print("fixture executable removed", flush=True)
    print("PASS: default service runtime ownership qualified on both nodes", flush=True)


if __name__ == "__main__":
    main()
