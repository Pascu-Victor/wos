#!/usr/bin/env python3
"""Exercise retained remote directory grants before/after owner rename and chmod."""
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
    parser.add_argument("--directory-paths", choices=("on", "off"), default="on")
    args = parser.parse_args()
    suffix = str(time.time_ns())
    binary = "/tmp/goal19-directory-paths-" + suffix
    base = "/tmp/goal19-directory-owner-" + suffix
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
                command = shlex.join([binary, "check", "/wki/wos-3" + base + "/hidden/anchor", ready, gate, args.directory_paths])
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
                    if status or error or "PASS remote directory paths checks=224" not in output:
                        raise RuntimeError(f"directory paths: status={status}, stderr={error!r}")
                print("PASS: 224 remote directory identity/grant checks", flush=True)
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
