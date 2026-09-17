#!/usr/bin/env python3
"""Run bounded process interval-timer semantics on an already running WOS VM."""
import argparse
import contextlib
import hashlib
import json
from pathlib import Path
import re
import shlex
import time

import paramiko

from service_ssh_broker_lifetime import connect
from service_time_load import run_command, snapshot


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("--username", default="root")
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    helper = "/tmp/goal31-interval-timers-" + str(time.time_ns())
    results = {"helper": helper, "removed": False, "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest()}

    def save():
        (args.output / "results.json").write_text(json.dumps(results, indent=2) + "\n")

    save()
    try:
        with contextlib.closing(paramiko.Agent()) as agent:
            keys = agent.get_keys()
            if not keys:
                raise RuntimeError("an authorized SSH agent key is required")
            with connect(args, keys[0]) as transport, paramiko.SFTPClient.from_transport(transport) as sftp:
                sftp.get_channel().settimeout(15)
                uploaded = False
                try:
                    try:
                        sftp.stat(helper)
                    except FileNotFoundError:
                        pass
                    else:
                        raise RuntimeError("refusing to overwrite existing helper")
                    uploaded = True
                    sftp.put(str(args.binary), helper)
                    sftp.chmod(helper, 0o755)
                    results["before"] = snapshot(transport, args.output, "before")
                    row = run_command(transport, shlex.join(["/usr/bin/locally", helper]), 180)
                    stdout, stderr = row.pop("stdout"), row.pop("stderr")
                    (args.output / "stdout.txt").write_text(stdout)
                    (args.output / "stderr.txt").write_text(stderr)
                    match = re.fullmatch(r"PASS interval timers checks=(\d+) failures=0\s*", stdout)
                    row["checks"] = int(match[1]) if match else 0
                    results["result"] = row
                    results["passed"] = row["status"] == 0 and row["checks"] >= 85 and not stderr
                    print(stdout, end="", flush=True)
                    if stderr:
                        print(stderr, end="", flush=True)
                finally:
                    if "before" in results:
                        results["after"] = snapshot(transport, args.output, "after")
                    if uploaded:
                        try:
                            sftp.remove(helper)
                            results["removed"] = True
                        except Exception as error:
                            results["cleanup_error"] = str(error)
                            results["passed"] = False
                    save()
    except Exception as error:
        results["error"] = str(error)
        results["passed"] = False
        save()
        raise
    return 0 if results.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
