#!/usr/bin/env python3
"""Verify receiver identity trust in a disposable four-node WOS cluster."""

import argparse
import contextlib
import shlex
import time

import paramiko

from service_ssh_broker_lifetime import connect


def run(transport, command):
    with transport.open_session(timeout=10) as channel:
        channel.settimeout(60)
        channel.exec_command(command)
        output = bytearray()
        while chunk := channel.recv(4096):
            output.extend(chunk)
            if len(output) > 32768:
                raise RuntimeError("unexpected fixture output size")
        error = channel.recv_stderr(4096).decode()
        return channel.recv_exit_status(), output.decode().strip(), error.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("--username", default="root")
    parser.add_argument("--binary", required=True)
    parser.add_argument("--unmapped", default="wos-1")
    parser.add_argument("--nonroot", default="wos-2")
    parser.add_argument("--root", default="wos-3")
    args = parser.parse_args()
    with contextlib.closing(paramiko.Agent()) as agent, contextlib.ExitStack() as stack:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        deadline = time.monotonic() + 120
        while True:
            try:
                transport = stack.enter_context(connect(args, keys[0]))
                break
            except (OSError, paramiko.SSHException):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1)
        deadline = time.monotonic() + 120
        while True:
            status, output, error = run(transport, "/usr/bin/wkictl auth status")
            rows = [line.split() for line in output.splitlines()]
            active = {int(row[0]) for row in rows if len(row) == 7 and row[0].isdecimal() and row[1] == "1"}
            peer_status, peer_output, _ = run(transport, "/usr/bin/cat /proc/wki/peers")
            peers = [line.split() for line in peer_output.splitlines()]
            connected = {row[0] for row in peers if len(row) == 7 and row[2] == "1"}
            targets = {args.unmapped, args.nonroot, args.root}
            ready = status == 0 and {2, 3, 4}.issubset(active) and peer_status == 0 and targets.issubset(connected)
            if ready:
                # Authentication precedes root pivot and resource publication.
                # Require payload visibility on the root-authorized receiver.
                # The other peers deliberately reject this caller's root
                # identity for VFS too; requiring access would prevent the
                # compute denial assertions below from ever running.
                path = shlex.quote(f"/wki/{args.root}/usr/bin/uname")
                ready &= run(transport, f"/usr/bin/test -x {path}")[0] == 0
            if ready:
                print(output, flush=True)
                print(peer_output, flush=True)
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(f"authenticated peers not ready: {status}, {output}, {error}")
            time.sleep(1)
        helper = "/tmp/goal19-wki-identity"
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.put(args.binary, helper)
            sftp.chmod(helper, 0o755)
            try:
                for iteration in range(3):
                    for fragmented in (False, True):
                        for target, expected in ((args.unmapped, 13), (args.nonroot, 13), (args.root, 0)):
                            suffix = "--fragment" if fragmented else "/usr/bin/uname -n"
                            status, output, error = run(transport, f"{helper} {shlex.quote(target)} {suffix}")
                            label = f"iteration={iteration} target={target} fragmented={fragmented}"
                            print(f"{label}: status={status} stdout={output!r} stderr={error!r}", flush=True)
                            if status != expected or error:
                                raise RuntimeError(f"unexpected receiver result: {label}")
                            if expected == 0 and output != args.root:
                                raise RuntimeError("accepted command did not execute on the receiver")
                            if expected == 13 and "errno=13" not in output:
                                raise RuntimeError("denial was not receiver EACCES")
                print("PASS: 18 authenticated receiver identity cases", flush=True)
            finally:
                sftp.remove(helper)


if __name__ == "__main__":
    main()
