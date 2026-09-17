#!/usr/bin/env python3
"""Exercise rejection delivery beside task completion and fragmented submission.

Requires the service_wki_identity topology: wos-2 accepts inert credentials
but rejects root and service authority. Use the service_wki_snapshot binary.
"""
import argparse
import contextlib

import paramiko

from service_ssh_broker_lifetime import connect
from service_wki_identity import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("--username", default="root")
    parser.add_argument("--binary", required=True)
    parser.add_argument("--iterations", type=int, default=100)
    args = parser.parse_args()
    if not 1 <= args.iterations <= 1000:
        parser.error("iterations must be between 1 and 1000")
    cases = (("root", "fragment", 13), ("inert", "ordinary", 0),
             ("inert", "fragment", 0), ("service", "ordinary", 13))
    path = "/tmp/goal19-wki-rejection-retry"
    count = 0
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            try:
                sftp.put(args.binary, path)
                sftp.chmod(path, 0o755)
                for iteration in range(args.iterations):
                    for profile, delivery, expected_status in cases:
                        label = f"iteration={iteration} profile={profile} delivery={delivery}"
                        print(f"BEGIN {label}", flush=True)
                        status, output, error = run(transport, f"{path} wos-2 {profile} {delivery} strict")
                        expected = ("remote exec rejected: errno=13" if expected_status else
                                    "wos-2 profile=inert snapshot=exact no-regain=yes")
                        if status != expected_status or output != expected or error:
                            raise RuntimeError(f"{label}: status={status} stdout={output!r} stderr={error!r}; "
                                               f"expected status={expected_status} stdout={expected!r}")
                        count += 1
                print(f"PASS: {count} interleaved submission/rejection cases", flush=True)
            finally:
                with contextlib.suppress(OSError):
                    sftp.remove(path)


if __name__ == "__main__":
    main()
