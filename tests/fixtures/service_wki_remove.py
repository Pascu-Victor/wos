#!/usr/bin/env python3
"""Check mapped remove parent/sticky policy and retained directory identity in a disposable WOS cluster."""
import argparse
import concurrent.futures
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
    args = parser.parse_args()
    suffix = str(time.time_ns())
    binary = "/tmp/goal19-remove-client-" + suffix
    base = args.base_root.rstrip("/") + "/goal19-remove-owner-" + suffix
    ready, gate = binary + ".ready", binary + ".go"
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.get_channel().settimeout(30)
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
                command = shlex.join([binary, "check", "/wki/wos-3" + base, ready, gate, "xfs" if args.base_root == "/" else "tmpfs"])
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    result = executor.submit(run, transport, command)
                    try:
                        deadline = time.monotonic() + 60
                        while True:
                            try:
                                sftp.stat(ready)
                                break
                            except FileNotFoundError:
                                if result.done() or time.monotonic() >= deadline:
                                    raise RuntimeError("fixture did not reach retained-descriptor barrier")
                                time.sleep(0.1)
                        owner("mutate")
                    finally:
                        with sftp.open(gate, "w") as stream:
                            stream.write("continue\n")
                    status, output, error = result.result(timeout=90)
                    print(output, flush=True)
                    if status or error or "PASS remove checks=512" not in output:
                        raise RuntimeError(f"remove: status={status}, stderr={error!r}")
                print("PASS: 512 remote remove identity checks", flush=True)
            finally:
                try:
                    owner("cleanup")
                finally:
                    for path in (ready, gate, binary):
                        try:
                            sftp.remove(path)
                        except FileNotFoundError:
                            pass
                print("fixture cleanup completed", flush=True)


if __name__ == "__main__":
    main()
