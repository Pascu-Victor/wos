#!/usr/bin/env python3
"""Verify native init-control authority and PID1 receipt of harmless unknown-service requests."""
import argparse
import contextlib
from pathlib import Path
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
    parser.add_argument("--serial-log", required=True, type=Path)
    args = parser.parse_args()
    offset = args.serial_log.stat().st_size
    path = "/tmp/goal19-init-control-" + str(time.time_ns())
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            try:
                sftp.put(args.binary, path)
                sftp.chmod(path, 0o755)
                status, output, error = run(transport, shlex.quote(path))
                print(output, flush=True)
                accepted = re.findall(r"^ACCEPT request=(\d+) pid=(\d+) service=([a-z0-9-]+)$", output, re.M)
                if status or error or output.count("PASS profile=") != 8 or len(accepted) != 9 or \
                        "PASS: 240 native init-control authority checks" not in output:
                    raise RuntimeError(f"init control: status={status}, stderr={error!r}")
                expected = {f"rejected control request {request} from pid {pid} for unknown/unavailable service {name}"
                            for request, pid, name in accepted}
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    with args.serial_log.open("rb") as log:
                        log.seek(offset)
                        text = log.read().decode(errors="replace")
                    missing = {line for line in expected if line not in text}
                    if not missing:
                        break
                    time.sleep(0.1)
                else:
                    raise RuntimeError(f"PID1 did not receive authenticated requests: {sorted(missing)}")
                print("PASS: PID1 received 9 requests with kernel-stamped sender PIDs; real services unchanged", flush=True)
            finally:
                with contextlib.suppress(OSError):
                    sftp.remove(path)
    print("fixture cleanup completed", flush=True)


if __name__ == "__main__":
    main()
