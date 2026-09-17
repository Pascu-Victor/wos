#!/usr/bin/env python3
"""Check pre-auth worker IDs and permanent-drop denials in a disposable WOS VM."""
import argparse
import contextlib
import socket
import time

import paramiko

from service_ssh_broker_lifetime import connect, inspect, run
from service_ssh_broker_policy import Files

HELPER = "/tmp/goal19-ssh-worker"


def workers(control):
    status, output = run(control, HELPER + " list")
    assert status == 0, output
    return {tuple(map(int, row.split())) for row in output.splitlines()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("--username", default="root")
    args = parser.parse_args()
    with contextlib.closing(paramiko.Agent()) as agent:
        key = agent.get_keys()[0]
        with connect(args, key) as control:
            baseline = workers(control)
            # Send no client identification or KEX packet. The server must have
            # dropped its worker before it can flush this initial banner.
            with socket.create_connection((args.host, 22), 10) as raw:
                raw.settimeout(10)
                with raw.makefile("rb") as stream:
                    assert stream.readline(512).startswith(b"SSH-2.")
                    added = workers(control) - baseline
                    assert len(added) == 1, added
                    pid, start = added.pop()
                    status, output = run(control, f"{HELPER} identity {pid} {start}")
                    assert status == 0 and "uid=104,104,104 gid=104,104,104" in output, output
            deadline = time.monotonic() + 10
            while (pid, start) in workers(control):
                if time.monotonic() >= deadline:
                    raise AssertionError("pre-auth connection did not release its worker")
                time.sleep(0.05)
            print("PASS dedicated worker IDs before any client identification/KEX input and pre-auth cleanup", flush=True)

            with connect(args, key) as target:
                worker, worker_start, broker, broker_start = inspect(target)
                status, output = run(control, f"{HELPER} identity {worker} {worker_start}")
                assert status == 0 and "uid=104,104,104 gid=104,104,104" in output, output
                print("PASS root authentication preserves dedicated worker IDs and broker ancestry", flush=True)
                status, output = run(target, f"{HELPER} session {worker} {worker_start} {broker} {broker_start}")
                assert status == 0 and "login session:" in output and "FAIL" not in output, output
                print(output, flush=True)
                status, output = run(control, f"{HELPER} probe {worker} {worker_start} {broker} {broker_start}")
                assert status == 0 and "worker policy:" in output and "worker set-ID exec:" in output and "FAIL" not in output, output
                print(output, flush=True)
                assert run(target, "printf session_survived") == (0, "session_survived")
                target.renegotiate_keys()
                assert inspect(target) == [worker, worker_start, broker, broker_start]
                assert run(target, "id -u; id -g") == (0, "0\n0")
                print("PASS policy probes preserve connection, broker, root login identity and rekey", flush=True)

            with paramiko.SFTPClient.from_transport(control) as sftp:
                files = Files(sftp)
                original = files.read("/etc/passwd")
                account = next(row for row in original.splitlines(keepends=True) if row.startswith(b"sshd:"))
                try:
                    fields = account.rstrip(b"\n").split(b":")
                    bad_uid, bad_gid = fields.copy(), fields.copy()
                    bad_uid[2], bad_gid[3] = b"0", b"0"
                    for replacement in (b"", b":".join(bad_uid) + b"\n", b":".join(bad_gid) + b"\n"):
                        files.replace("/etc/passwd", original.replace(account, replacement))
                        with socket.create_connection((args.host, 22), 10) as raw:
                            raw.settimeout(10)
                            assert raw.recv(1) == b"", "invalid worker identity accepted a connection"
                        assert run(control, "printf retained_control") == (0, "retained_control")
                finally:
                    files.restore()
            with connect(args, key) as recovered:
                assert run(recovered, "printf recovered") == (0, "recovered")
            print("PASS missing/root worker identities fail before input; original passwd restored and new login recovers", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
