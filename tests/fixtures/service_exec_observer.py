#!/usr/bin/env python3
"""Verify authorized procfs observers see the committed image throughout refused remote exec."""
import argparse
import contextlib
import time

import paramiko

from service_ssh_broker_lifetime import connect
from service_wki_identity import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("--username", default="root")
    parser.add_argument("--binary", required=True)
    parser.add_argument("--payload", required=True)
    parser.add_argument("--interpreter", required=True)
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
        root = "/tmp/goal19-exec-observer"
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.mkdir(root, 0o755)
            try:
                for source, name, mode in ((args.binary, "check", 0o755), (args.payload, "payload", 0o755),
                                           (args.interpreter, "ld.so", 0o644)):
                    sftp.put(source, f"{root}/{name}")
                    sftp.chmod(f"{root}/{name}", mode)
                status, output, error = run(transport, f"{root}/check /wki/wos-0{root}/payload")
                print(f"status={status} stdout={output!r} stderr={error!r}", flush=True)
                if status != 0 or error:
                    raise RuntimeError("exec observer consistency failed")
            finally:
                for name in ("check", "payload", "ld.so"):
                    with contextlib.suppress(OSError):
                        sftp.remove(f"{root}/{name}")
                sftp.rmdir(root)


if __name__ == "__main__":
    main()
