#!/usr/bin/env python3
"""Check real/effective ACCESS identities using WOS's supported set-ID exec ABI."""
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
    parser.add_argument("--filesystem", choices=("tmpfs", "xfs"), default="xfs")
    parser.add_argument("--remote-owner", help="Use this WOS peer's fixture files")
    args = parser.parse_args()
    suffix = str(time.time_ns())
    # The seed executable uses the source's XFS mount so set-ID is supported.
    binary = "/goal19-access-identity-" + suffix + "-bin"
    base = ("/tmp/" if args.filesystem == "tmpfs" else "/") + "goal19-access-identity-" + suffix
    if args.remote_owner:
        base = "/wki/" + args.remote_owner + base
    print(f"fixture binary={binary} base={base}", flush=True)
    with contextlib.closing(paramiko.Agent()) as agent:
        keys = agent.get_keys()
        if not keys:
            raise RuntimeError("an authorized SSH agent key is required")
        with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.put(args.binary, binary)
            sftp.chmod(binary, 0o4755)
            try:
                status, output, error = run(transport, shlex.join(["/usr/bin/locally", binary, base]))
                print(output, flush=True)
                if status or error or output.count("PASS ") != 10 or "ACCESS_IDENTITY profiles_failed=0 cleanup=1" not in output:
                    raise RuntimeError(f"ACCESS identity: status={status}, stderr={error!r}")
                try:
                    sftp.stat(base)
                except FileNotFoundError:
                    pass
                else:
                    raise RuntimeError("fixture directory still present")
            finally:
                sftp.remove(binary)
            print("fixture cleanup completed", flush=True)


if __name__ == "__main__":
    main()
