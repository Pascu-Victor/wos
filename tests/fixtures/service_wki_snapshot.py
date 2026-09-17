#!/usr/bin/env python3
"""Full credential matrix after service_wki_identity.py verifies export readiness.

With --legacy-root, receiver wos-3 must boot with wki.compute_credentials_off.
The same binary then checks strict ENOTSUP and optional local execution.
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
    parser.add_argument("--large-binary", required=True)
    parser.add_argument("--legacy-root", action="store_true")
    parser.add_argument("--fd-metadata-off", action="store_true", help="receiver 3 disables retained-FD metadata negotiation")
    parser.add_argument("--receiver-only", choices=("allow", "deny"),
                        help="test only wos-3 with the declared identity policy allowing or denying every profile")
    args = parser.parse_args()
    if args.legacy_root and args.fd_metadata_off:
        parser.error("select only one mixed capability mode")
    if args.receiver_only and (args.legacy_root or args.fd_metadata_off):
        parser.error("receiver-only identity expectations cannot be combined with mixed capability modes")
    count = 0
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            installed = []
            try:
                for source, size in ((args.binary, "small"), (args.large_binary, "large")):
                    path = f"/tmp/goal19-wki-snapshot-{size}"
                    sftp.put(source, path)
                    installed.append(path)
                    sftp.chmod(path, 0o755)
                    for profile in ("root", "inert", "service"):
                        for delivery in ("ordinary", "fragment"):
                            targets = ("wos-3",) if args.legacy_root or args.receiver_only else ("wos-1", "wos-2", "wos-3")
                            for target in targets:
                                modes = ("strict", "optional") if args.legacy_root else ("strict",)
                                for mode in modes:
                                    allowed = target == "wos-3" or (target == "wos-2" and profile == "inert")
                                    result_node = target
                                    expected_status = 0 if allowed else 13
                                    if args.receiver_only == "deny":
                                        expected_status = 13
                                    if args.legacy_root:
                                        expected_status = 95 if mode == "strict" else 0
                                        result_node = "wos-0"
                                    if args.fd_metadata_off and target == "wos-3":
                                        expected_status = 95
                                    expected = (
                                        f"remote exec rejected: errno={expected_status}" if expected_status else
                                        f"{result_node} profile={profile} snapshot=exact no-regain=yes"
                                    )
                                    status, output, error = run(transport, f"{path} {target} {profile} {delivery} {mode}")
                                    label = f"{size} {profile} {delivery} {target} {mode}"
                                    print(f"{label}: status={status} stdout={output!r} stderr={error!r}", flush=True)
                                    if error or status != expected_status or output != expected:
                                        raise RuntimeError(f"snapshot mismatch: {label}; expected {expected_status}, {expected!r}")
                                    count += 1
                print(f"PASS: {count} full snapshot/no-regain cases", flush=True)
            finally:
                for path in installed:
                    with contextlib.suppress(OSError):
                        sftp.remove(path)


if __name__ == "__main__":
    main()
