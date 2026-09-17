#!/usr/bin/env python3
"""Check explicit FAT32 PT_INTERP reads on a disposable WOS cluster.

The payload must be linked with PT_INTERP=/boot/g19ld.so. Run the identity
fixture first to establish receiver export readiness. All writes affect VM
overlays; existing fixture paths cause an error instead of being overwritten.
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
    parser.add_argument("--launcher", required=True)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--large-binary", required=True)
    parser.add_argument("--remote-binary", required=True,
                        help="payload linked with PT_INTERP=/wki/wos-0/boot/g19ld.so")
    parser.add_argument("--directory-binary", required=True,
                        help="payload linked with PT_INTERP=/boot/EFI")
    parser.add_argument("--remote-directory-binary", required=True,
                        help="payload linked with PT_INTERP=/wki/wos-0/boot/EFI")
    parser.add_argument("--interpreter", required=True)
    args = parser.parse_args()
    count = 0
    failures = []
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.get_channel().settimeout(30)
            installed = []

            def install(source, path, executable=False):
                try:
                    sftp.stat(path)
                except OSError as error:
                    if error.errno != 2:
                        raise
                else:
                    raise RuntimeError(f"fixture path already exists: {path}")
                sftp.put(source, path)
                installed.append(path)
                if executable:
                    sftp.chmod(path, 0o755)

            def check(target, mode, payload, profile, delivery, failure=False):
                nonlocal count
                status, output, error = run(transport, f"/tmp/g19fat-launch {target} {profile} {delivery} {mode} {payload}")
                # Local SPAWN maps a zero kernel construction result to EAGAIN;
                # remote construction reports the existing EIO rejection.
                expected_status = (11 if mode == "local" else 5) if failure else 0
                expected = (f"remote exec rejected: errno={expected_status}" if failure else
                            f"{target} profile={profile} snapshot=exact no-regain=yes")
                print(f"{target} {payload} {profile} {delivery} failure={failure}: status={status} stdout={output!r} stderr={error!r}", flush=True)
                if (status, output, error) != (expected_status, expected, ""):
                    failures.append(f"{target} {payload} {profile} {delivery} failure={failure}: {status}, {output!r}, {error!r}")
                count += 1

            try:
                install(args.launcher, "/tmp/g19fat-launch", True)
                for source, size in ((args.binary, "small"), (args.large_binary, "large")):
                    install(source, f"/tmp/g19fat-{size}", True)
                install(args.remote_binary, "/tmp/g19fat-remote", True)
                install(args.directory_binary, "/tmp/g19fat-dir", True)
                install(args.remote_directory_binary, "/tmp/g19fat-remotedir", True)
                for target, mode, owner, sizes in (
                        ("wos-0", "local", "", ("small", "large")),
                        ("wos-3", "strict-local-boot", "/wki/wos-3", ("small", "large")),
                        ("wos-3", "strict", "", ("remote",))):
                    interpreter = f"{owner}/boot/g19ld.so"
                    install(args.interpreter, interpreter)
                    for size in sizes:
                        for profile in ("root", "inert", "service"):
                            for delivery in ("ordinary", "fragment"):
                                check(target, mode, f"/tmp/g19fat-{size}", profile, delivery)
                    sftp.remove(interpreter)
                    installed.remove(interpreter)
                    payload = f"/tmp/g19fat-{sizes[0]}"
                    check(target, mode, payload, "inert", "ordinary", True)
                    status, output, error = run(transport, f"/tmp/g19fat-launch --mkdir-denial {interpreter}")
                    print(f"mkdir {interpreter}: status={status} stdout={output!r} stderr={error!r}", flush=True)
                    if (status, output, error) != (0, "PASS: unsupported FAT mkdir left path absent", ""):
                        raise RuntimeError("unsupported FAT mkdir did not return without side effects")
                    directory_payload = "/tmp/g19fat-remotedir" if sizes == ("remote",) else "/tmp/g19fat-dir"
                    check(target, mode, directory_payload, "inert", "fragment", True)
                    install(args.interpreter, interpreter)
                    check(target, mode, payload, "inert", "ordinary")
                    sftp.remove(interpreter)
                    installed.remove(interpreter)
                if failures:
                    raise RuntimeError(f"{len(failures)} of {count} FAT interpreter cases failed: " + "; ".join(failures))
                print(f"PASS: {count} FAT interpreter snapshot, missing/directory denial and recovery cases", flush=True)
            finally:
                for path in reversed(installed):
                    with contextlib.suppress(OSError):
                        sftp.remove(path)


if __name__ == "__main__":
    main()
