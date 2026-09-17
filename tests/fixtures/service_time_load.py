#!/usr/bin/env python3
"""Qualify clocks and timers on an existing optimized WOS VM; never launch a VM."""
import argparse
import contextlib
import hashlib
import json
from pathlib import Path
import shlex
import time

import paramiko

from service_ssh_broker_lifetime import connect

OUTPUT_LIMIT = 262144
DRIFT_PPM = 1000


def run_command(transport, command, timeout):
    started = time.perf_counter_ns()
    deadline = time.monotonic() + timeout
    output, error = bytearray(), bytearray()
    with transport.open_session(timeout=10) as channel:
        channel.settimeout(10)
        channel.exec_command(command)
        while True:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"command exceeded {timeout}s: {command}")
            while channel.recv_ready():
                output.extend(channel.recv(4096))
                if len(output) > OUTPUT_LIMIT:
                    raise RuntimeError("stdout exceeded bound")
            while channel.recv_stderr_ready():
                error.extend(channel.recv_stderr(4096))
                if len(error) > OUTPUT_LIMIT:
                    raise RuntimeError("stderr exceeded bound")
            if channel.exit_status_ready() and not channel.recv_ready() and not channel.recv_stderr_ready():
                status = channel.recv_exit_status()
                break
            time.sleep(0.005)
    return {"command": command, "status": status, "host_elapsed_ns": time.perf_counter_ns() - started,
            "stdout": output.decode(errors="replace"), "stderr": error.decode(errors="replace")}


class Lines:
    def __init__(self, channel):
        self.channel = channel
        self.pending = bytearray()
        self.total = 0

    def receive(self):
        while b"\n" not in self.pending:
            chunk = self.channel.recv(4096)
            if not chunk:
                raise RuntimeError("clock sample channel closed before a complete response")
            self.total += len(chunk)
            if self.total > OUTPUT_LIMIT:
                raise RuntimeError("clock sample output exceeded bound")
            self.pending.extend(chunk)
        line, _, tail = self.pending.partition(b"\n")
        self.pending = bytearray(tail)
        return line.decode().strip()


def drift_bounds(first, last):
    guest_ns = last["guest_ns"] - first["guest_ns"]
    low_ns = last["send_ns"] - first["receive_ns"]
    high_ns = last["receive_ns"] - first["send_ns"]
    if guest_ns <= 0 or low_ns <= 0:
        return {"status": "FAIL", "reason": "nonpositive measured elapsed time"}
    lower = (guest_ns / high_ns - 1) * 1e6
    upper = (guest_ns / low_ns - 1) * 1e6
    uncertainty = (upper - lower) / 2
    if uncertainty > DRIFT_PPM:
        status = "INCONCLUSIVE"
    elif lower >= -DRIFT_PPM and upper <= DRIFT_PPM:
        status = "PASS"
    elif upper < -DRIFT_PPM or lower > DRIFT_PPM:
        status = "FAIL"
    else:
        status = "INCONCLUSIVE"
    return {"status": status, "guest_elapsed_ns": guest_ns, "host_elapsed_lower_ns": low_ns,
            "host_elapsed_upper_ns": high_ns, "drift_lower_ppm": lower, "drift_upper_ppm": upper,
            "rtt_uncertainty_ppm": uncertainty, "threshold_ppm": DRIFT_PPM}


def measure_drift(transport, command, seconds):
    with transport.open_session(timeout=10) as channel:
        channel.settimeout(5)
        channel.exec_command(command)
        lines = Lines(channel)
        if lines.receive() != "READY":
            raise RuntimeError("clock sampling helper did not become ready")

        def endpoint():
            samples = []
            for _ in range(16):
                before = time.perf_counter_ns()
                channel.sendall(b"S")
                response = lines.receive()
                after = time.perf_counter_ns()
                if not response.isdecimal():
                    raise RuntimeError("malformed clock sample: " + response)
                samples.append({"send_ns": before, "receive_ns": after, "guest_ns": int(response),
                                "rtt_ns": after - before})
            return min(samples, key=lambda sample: sample["rtt_ns"]), samples

        first, first_samples = endpoint()
        time.sleep(seconds)
        last, last_samples = endpoint()
        channel.sendall(b"Q")
        channel.shutdown_write()
        deadline = time.monotonic() + 10
        while not channel.exit_status_ready():
            if time.monotonic() >= deadline:
                raise TimeoutError("clock sample helper did not exit")
            time.sleep(0.005)
        status = channel.recv_exit_status()
        stderr = bytearray()
        while channel.recv_stderr_ready():
            stderr.extend(channel.recv_stderr(4096))
            if len(stderr) > OUTPUT_LIMIT:
                raise RuntimeError("clock sampling stderr exceeded bound")
        if status or stderr:
            raise RuntimeError(f"clock sample helper failed: status={status}, stderr={stderr!r}")
    return {**drift_bounds(first, last), "first": first, "last": last,
            "first_samples": first_samples, "last_samples": last_samples}


def snapshot(transport, output, label):
    result = {}
    for name in ("timekeeping", "kcpustat"):
        try:
            # The WOS SFTP server treats synthetic files with st_size == 0 as
            # empty. Read through cat, which consumes procfs until read EOF.
            row = run_command(transport, shlex.join(["/usr/bin/locally", "cat", "/proc/" + name]), 20)
            if row["status"] != 0 or row["stderr"] or not row["stdout"]:
                raise RuntimeError("proc snapshot failed: " + repr(row))
            data = row["stdout"].encode()
            (output / f"{label}-{name}.txt").write_bytes(data)
            result[name] = {"bytes": len(data), "captured": True}
        except Exception as error:
            result[name] = {"captured": False, "error": str(error)}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("--username", default="root")
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--profile", required=True, choices=("normal", "diagnostic"))
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--timeout", type=float, default=240)
    parser.add_argument("--seconds", type=float, default=10)
    args = parser.parse_args()
    if not 10 <= args.seconds <= 60 or not 190 <= args.timeout <= 600:
        parser.error("seconds must be 10..60 and timeout 190..600")
    args.output.mkdir(parents=True, exist_ok=False)
    helper = "/tmp/goal31-time-load-" + str(time.time_ns())
    results = {"host": args.host, "profile": args.profile, "helper": helper, "removed": False,
               "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(), "modes": {},
               "requested_drift_seconds": args.seconds, "command_timeout_seconds": args.timeout,
               "thresholds": {"monotonic_threads": 8, "monotonic_reads": 1000000,
                              "latency_p99_ns": 5000000, "latency_max_ns": 50000000,
                              "storm_threads": 128, "storm_p99_ns": 100000000,
                              "storm_max_ns": 1000000000, "drift_ppm": DRIFT_PPM}}

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
                        raise RuntimeError("refusing to overwrite existing helper " + helper)
                    uploaded = True  # Also remove a partially uploaded file on failure.
                    sftp.put(str(args.binary), helper)
                    sftp.chmod(helper, 0o755)
                    results["before"] = snapshot(transport, args.output, "before")
                    save()
                    for mode in ("monotonic", "latency", "storm"):
                        row = run_command(transport, shlex.join(["/usr/bin/locally", helper, mode]), args.timeout)
                        (args.output / f"{mode}-stdout.txt").write_text(row.pop("stdout"))
                        (args.output / f"{mode}-stderr.txt").write_text(row.pop("stderr"))
                        # Persist exit status even when a crash produces no JSON.
                        results["modes"][mode] = row
                        save()
                        row["measurement"] = json.loads((args.output / f"{mode}-stdout.txt").read_text())
                        row["passed"] = row["status"] == 0 and row["measurement"].get("passed") is True and not (
                            args.output / f"{mode}-stderr.txt").read_text()
                        results["modes"][mode] = row
                        print(mode + ": " + json.dumps(row["measurement"]), flush=True)
                        save()
                    results["drift"] = measure_drift(
                        transport, shlex.join(["/usr/bin/locally", helper, "sample-loop"]), args.seconds)
                    print("drift: " + results["drift"]["status"], flush=True)
                    results["after"] = snapshot(transport, args.output, "after")
                    passed = all(row["passed"] for row in results["modes"].values())
                    diagnostics = all(row["captured"] for label in ("before", "after") for row in results[label].values())
                    if not passed or results["drift"]["status"] == "FAIL":
                        results["status"] = "FAIL"
                    elif args.profile != "normal" or not diagnostics or results["drift"]["status"] != "PASS":
                        results["status"] = "INCONCLUSIVE"
                    else:
                        results["status"] = "PASS"
                finally:
                    if "before" in results and "after" not in results:
                        results["after"] = snapshot(transport, args.output, "after")
                    if uploaded:
                        try:
                            sftp.remove(helper)
                            results["removed"] = True
                        except Exception as error:
                            results["cleanup_error"] = str(error)
                            results["status"] = "FAIL"
                    save()
    except Exception as error:
        results["error"] = str(error)
        results["status"] = "FAIL"
        save()
        raise
    print(results["status"] + ": evidence in " + str(args.output), flush=True)
    return {"PASS": 0, "FAIL": 1, "INCONCLUSIVE": 2}[results["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
