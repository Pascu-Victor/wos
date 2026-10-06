#!/usr/bin/env python3
"""Private cold-file storage qualification using the shared rootless VM launcher.

Only fresh ktest-data/goal30-* paths are written. Source images are read-only
inputs and must be idle during preparation. Every launch uses the ordinary
exclusive cluster guard and --no-setup. No existing cluster is stopped.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shlex
import signal
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/cluster"))
import node_setup

BYTES_PER_FILE = 64 * 1024 * 1024
BLOCK_BYTES = 128 * 1024
FSYNC_OPERATIONS = 100
METADATA_OPERATIONS = 1000
PATTERN = 0x5a
PROFILE = {"version": 1, "pairs": 5, "read_bytes_per_worker": BYTES_PER_FILE, "read_block_bytes": BLOCK_BYTES,
           "qd1_workers": 1, "qd8_workers": 8, "fsync_operations": FSYNC_OPERATIONS, "fsync_bytes": 4096,
           "metadata_operations": METADATA_OPERATIONS, "disk_cache": "writeback", "memory": "2G", "cpus": 8,
           "qd8_min_ratio": 1.15, "qd1_min_ratio": 0.95, "metadata_min_ratio": 0.95, "fsync_p95_max_ratio": 1.10}


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + "\n")


def fresh(path):
    path = Path(path).resolve()
    if not path.is_relative_to(ROOT / "ktest-data") or not path.name.startswith("goal30-"):
        raise ValueError("output must be a fresh ktest-data/goal30-* directory")
    path.mkdir(parents=True, exist_ok=False)
    return path


def command(arguments, work, log, **kwargs):
    with (work / log).open("w") as output:
        subprocess.run(list(map(str, arguments)), cwd=ROOT, env=dict(os.environ, TMPDIR=str(work)),
                       stdout=output, stderr=subprocess.STDOUT, check=True, **kwargs)


def prepare_media(args):
    work = fresh(args.output)
    media = work / "root-base.qcow2"
    command(["cp", "--reflink=auto", "--sparse=always", args.rootfs_disk, media], work, "media-copy.log")
    pattern = work / "pattern.bin"
    with pattern.open("wb") as stream:
        chunk = bytes([PATTERN]) * (1024 * 1024)
        for _ in range(BYTES_PER_FILE // len(chunk)):
            stream.write(chunk)
    lines = ["run", "mount /dev/sda1 /", "mkdir-p /var/tmp/blockbench-data"]
    # Every file is fully uploaded/materialized, rather than a sparse or
    # unwritten extent that XFS could satisfy without issuing disk reads.
    for name in ["qd1"] + [f"qd8.{index}" for index in range(8)]:
        lines.append(f"upload {shlex.quote(str(pattern))} /var/tmp/blockbench-data/{name}")
    lines += [f"upload {shlex.quote(str(args.testprog))} /usr/bin/testprog", "chmod 0755 /usr/bin/testprog", "umount /"]
    command(["guestfish", "--rw", "-a", media], work, "media-seed.log", input="\n".join(lines) + "\n", text=True)
    save(work / "provenance.json", {"profile": PROFILE, "source_rootfs_sha256": digest(args.rootfs_disk),
                                    "testprog_sha256": digest(args.testprog), "media_sha256": digest(media),
                                    "pattern_sha256": digest(pattern), "runner_sha256": digest(__file__)})
    pattern.unlink()
    print(media, flush=True)


def guest_script():
    lines = ["#!/bin/sh", "set -eu", "PATH=/usr/bin:/usr/sbin:/bin:/sbin", "export PATH", "mkdir -p /var/tmp",
             "exec >/var/tmp/block-qualification.log 2>&1", "failed=0",
             "trap 'echo BLOCKREPORT-BEGIN >/dev/ttyS0; cat /var/tmp/block-qualification.log >/dev/ttyS0; echo BLOCKREPORT-END >/dev/ttyS0; sync; poweroff' EXIT",
             "echo BLOCKQUEUE-before", "if [ -f /proc/blockq ]; then cat /proc/blockq; fi"]
    commands = [
        ("qd1", f"testprog blockbench-read --path /var/tmp/blockbench-data/qd1 --workers 1 --bytes {BYTES_PER_FILE} --block-bytes {BLOCK_BYTES}"),
        ("qd8", f"testprog blockbench-read --path /var/tmp/blockbench-data/qd8 --path-prefix --workers 8 --bytes {BYTES_PER_FILE} --block-bytes {BLOCK_BYTES}"),
        ("create", f"testprog vfsbench-create --path /var/tmp/blockbench-create --iterations {METADATA_OPERATIONS} --workers 1"),
        ("rename", f"testprog vfsbench-rename --path /var/tmp/blockbench-rename --iterations {METADATA_OPERATIONS} --workers 1"),
        ("fsync", f"testprog blockbench-fsync --path /var/tmp/blockbench-fsync --operations {FSYNC_OPERATIONS}"),
    ]
    for name, value in commands:
        lines += [f"echo BLOCKCASE={name}", f"if {value}; then echo BLOCKSTATUS=0; else echo BLOCKSTATUS=1; failed=1; fi"]
    lines += ["echo BLOCKQUEUE-after", "if [ -f /proc/blockq ]; then cat /proc/blockq; fi",
              'echo BLOCKDONE-failed=$failed', "echo BLOCKREPORT-BEGIN >/dev/ttyS0",
              "cat /var/tmp/block-qualification.log >/dev/ttyS0", "echo BLOCKREPORT-END >/dev/ttyS0",
              "trap - EXIT", "sync", "poweroff"]
    return "\n".join(lines) + "\n"


def manifest():
    return """version=1
[service journald]
user=journald
group=journald
supplementary-groups=none
umask=0077
authority-ceiling=16384
no-new-privileges=yes
service-privileges=journal-read
private-state=journal
exec=/sbin/journald
arg=/sbin/journald
type=daemon
class=journal
priority=10
environment=empty
readiness=immediate
readiness-timeout-ms=5000
restart=never
backoff-initial-ms=250
backoff-max-ms=30000
restart-budget=5
restart-window-ms=60000
stable-run-ms=60000
stop-term-ms=2000
stop-kill-ms=1000
drain-timeout-ms=2000
stdio=null
enable=always

[service block-qualification]
user=root
group=root
supplementary-groups=none
umask=0077
authority-ceiling=65535
no-new-privileges=no
service-privileges=none
exec=/bin/sh
arg=/bin/sh
arg=/usr/libexec/wos-block-qualification.sh
depends=journald
type=oneshot
class=normal
priority=0
environment=init
readiness=exit-success
readiness-timeout-ms=600000
restart=never
backoff-initial-ms=250
backoff-max-ms=30000
restart-budget=5
restart-window-ms=60000
stable-run-ms=60000
stop-term-ms=2000
stop-kill-ms=1000
drain-timeout-ms=2000
stdio=inherit
enable=always
"""


def prepare_run(args, work):
    raw = work / "boot.raw"
    boot = work / "boot.qcow2"
    command(["qemu-img", "convert", "-f", "qcow2", "-O", "raw", args.boot_disk, raw], work, "boot-convert.log")
    image = f"{raw}@@1048576"
    limine = work / "limine.conf"
    command(["mcopy", "-o", "-i", image, "::/limine/limine.conf", limine], work, "limine-extract.log")
    text = limine.read_text()
    if not re.search(r"^\s*cmdline:", text, re.M):
        raise ValueError("boot image has no kernel command line")
    limine.write_text(re.sub(r"^(\s*cmdline:).*", r"\1", text, flags=re.M))
    command(["mcopy", "-o", "-i", image, args.kernel, "::/wos"], work, "kernel-inject.log")
    command(["mcopy", "-o", "-i", image, limine, "::/limine/limine.conf"], work, "limine-inject.log")
    packaged = work / "packaged-wos"
    command(["mcopy", "-o", "-i", image, "::/wos", packaged], work, "kernel-verify.log")
    if digest(packaged) != digest(args.kernel):
        raise ValueError("packaged kernel hash mismatch")
    command(["qemu-img", "convert", "-f", "raw", "-O", "qcow2", raw, boot], work, "boot-pack.log")
    raw.unlink()
    packaged.unlink()
    disk = work / "root-base.qcow2"
    command(["qemu-img", "create", "-f", "qcow2", "-F", "qcow2", "-b", args.media_disk, disk], work, "root-create.log")
    script = work / "guest.sh"
    script.write_text(guest_script())
    service = work / "wos-services.conf"
    service.write_text(manifest())
    commands = f"run\nmount /dev/sda1 /\nmkdir-p /usr/libexec\nupload {shlex.quote(str(script))} /usr/libexec/wos-block-qualification.sh\nchmod 0755 /usr/libexec/wos-block-qualification.sh\nupload {shlex.quote(str(service))} /etc/wos-services.conf\numount /\n"
    command(["guestfish", "--rw", "-a", disk], work, "root-inject.log", input=commands, text=True)
    spec = {"id": 0, "hostname": "goal30-storage", "debug": False, "mtu": 1500,
            "vm": {"memory": PROFILE["memory"], "cpus": PROFILE["cpus"], "disk_cache": PROFILE["disk_cache"],
                   "disk0": str(boot), "disk1": str(disk), "overlay_dir": str(work / "overlays"),
                   "serial_log": str(work / "serial.log"), "qemu_log": str(work / "qemu.log"),
                   "qmp_socket": str(work / "qmp.sock"), "usb_hotplug": {"enabled": False}},
            "nics": [{"name": "storage", "zone_id": 30, "tap": "wos-goal30-N0", "mac": "52:54:00:30:00:00",
                      "model": "virtio-net-pci", "queues": 1, "vhost": False, "driver": "unmanaged",
                      "qemu_netdev": {"type": "socket-mcast", "address": "239.192.30.1", "port": 47930,
                                      "localaddr": "127.0.0.1"}, "bridge": {"ip": None}}]}
    save(work / "node.json", spec)
    save(work / "cluster.json", node_setup.cluster_config_from_node_spec(spec))
    provenance = {"profile": PROFILE, "kernel_sha256": digest(args.kernel), "boot_sha256": digest(boot),
                  "source_boot_sha256": digest(args.boot_disk), "source_media_sha256": digest(args.media_disk),
                  "media_provenance": json.loads(args.media_disk.with_name("provenance.json").read_text()),
                  "guest_sha256": digest(script), "runner_sha256": digest(__file__)}
    save(work / "provenance.json", provenance)


def parse_report(text):
    results, queue_rows = {}, []
    current = None
    for line in text.splitlines():
        if line.startswith("BLOCKCASE="):
            current = line.partition("=")[2]
            if current in results:
                raise ValueError("duplicate benchmark case")
            results[current] = {"status": None, "data": None}
        elif line.startswith("BLOCKSTATUS=") and current is not None:
            results[current]["status"] = int(line.partition("=")[2])
        elif line.startswith("{") and current is not None:
            if results[current]["data"] is not None:
                raise ValueError("duplicate result")
            results[current]["data"] = json.loads(line)
        elif line.startswith("device="):
            queue_rows.append(dict(part.split("=", 1) for part in line.split() if "=" in part))
    if set(results) != {"qd1", "qd8", "create", "rename", "fsync"} or "BLOCKDONE-failed=0" not in text:
        raise ValueError("incomplete or failed guest report")
    for name, result in results.items():
        if result["status"] != 0 or result["data"] is None:
            raise ValueError(f"failed/missing {name} result")
        data = result["data"]
        if name in ("qd1", "qd8"):
            workers = 1 if name == "qd1" else 8
            if not data["pass"] or data["workers"] != workers or data["bytes"] != BYTES_PER_FILE * workers:
                raise ValueError(f"invalid {name} exact-byte result")
            if data["block_bytes"] != BLOCK_BYTES or data["pattern"] != PATTERN or data["elapsed_ns"] <= 0:
                raise ValueError(f"changed {name} profile")
        elif name == "fsync":
            if not data["pass"] or data["operations"] != FSYNC_OPERATIONS or len(data["latencies_ns"]) != FSYNC_OPERATIONS:
                raise ValueError("invalid fsync sample count")
            if data["p95_ns"] != sorted(data["latencies_ns"])[94]:
                raise ValueError("incorrect nearest-rank fsync p95")
        elif data["iterations"] != METADATA_OPERATIONS or data["workers"] != 1 or data["elapsed_seconds"] <= 0:
            raise ValueError("changed metadata profile")
    return results, queue_rows


def shutdown_receipt(serial):
    failures = re.findall(r"[^\n]*(?:kernel panic|PANIC:|KASAN:|UBSAN:|shutdown continuing with|shutdown vfs sync returned|"
                          r"shutdown root/mount unmount returned|AHCI shutdown returned)[^\n]*", serial, re.I)
    if failures:
        raise ValueError(f"kernel/shutdown diagnostic failure: {failures[:4]}")
    retired = re.search(r"ahci: DMA retirement result=(\d+) bme_disabled=(\d+) regions=(\d+)->(\d+) pages=(\d+)->(\d+) quarantined=(\d+)", serial)
    if retired is None:
        raise ValueError("missing AHCI poweroff retirement receipt")
    values = list(map(int, retired.groups()))
    if values[0] != 0 or values[1] != 1 or values[3] != 0 or values[5] != 0 or values[6] != 0:
        raise ValueError("AHCI poweroff did not retire every DMA region/page")
    finalizer = serial.find("system shutdown finalizer starting")
    stopped = serial.find("shutdown requested stop for")
    if finalizer < 0 or stopped < finalizer or stopped >= retired.start():
        raise ValueError("kernel worker stop did not precede hardware retirement")
    return {"result": values[0], "bme_disabled": values[1], "regions_before": values[2], "regions_after": values[3],
            "pages_before": values[4], "pages_after": values[5], "quarantined": values[6], "workers_stopped_before_hardware": True}


def cold_verify(work):
    output = work / "cold-fsync.bin"
    overlay = work / "overlays/mountfs-vm0.qcow2"
    commands = f"run\nmount-ro /dev/sda1 /\ndownload /var/tmp/blockbench-fsync {shlex.quote(str(output))}\numount /\n"
    command(["guestfish", "--ro", "-a", overlay], work, "cold-fsync-extract.log", input=commands, text=True)
    data = output.read_bytes()
    if len(data) != FSYNC_OPERATIONS * 4096:
        raise ValueError("cold-recovered fsync file has incorrect length")
    for index in range(FSYNC_OPERATIONS):
        if data[index * 4096:(index + 1) * 4096] != bytes([(PATTERN + index) & 255]) * 4096:
            raise ValueError(f"cold-recovered fsync file is corrupt at operation {index}")
    return {"bytes": len(data), "operations": FSYNC_OPERATIONS, "sha256": digest(output), "pass": True,
            "kind": "cold media verification after orderly VM poweroff"}


def launch(args, work):
    receipt = {"completed": False, "errors": [], "provenance": json.loads((work / "provenance.json").read_text())}
    launcher = None
    try:
        if receipt["provenance"]["runner_sha256"] != digest(__file__):
            raise ValueError("runner changed after fixture preparation")
        if receipt["provenance"]["kernel_sha256"] != digest(args.kernel):
            raise ValueError("kernel changed after preparation")
        with (work / "launch.log").open("w") as output:
            launcher = subprocess.Popen([str(ROOT / "bin/wos-cluster"), "--config", str(work / "cluster.json"),
                                         "--launch", "--no-setup", "--live-debug-descriptor", str(work / "live-debug.json")],
                                        cwd=ROOT, env=dict(os.environ, TMPDIR=str(work)), stdout=output,
                                        stderr=subprocess.STDOUT, start_new_session=True)
            launcher.wait(timeout=args.timeout_s)
        if launcher.returncode != 0:
            raise ValueError("owned launcher failed")
        serial = (work / "serial.log").read_text(errors="replace")
        if serial.count("BLOCKREPORT-BEGIN") != 1 or serial.count("BLOCKREPORT-END") != 1:
            raise ValueError("missing unique terminal serial receipt")
        text = serial.split("BLOCKREPORT-BEGIN", 1)[1].split("BLOCKREPORT-END", 1)[0]
        (work / "guest-report.log").write_text(text)
        receipt["results"], receipt["queue_snapshots"] = parse_report(text)
        receipt["identify"] = re.findall(r"[^\n]*IDENTIFY[^\n]*", serial)
        receipt["shutdown"] = shutdown_receipt(serial)
        receipt["cold_recovery"] = cold_verify(work)
        receipt["completed"] = True
    except Exception as error:
        receipt["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        if launcher is not None and launcher.poll() is None:
            # Terminate only the process group created by this invocation.
            os.killpg(launcher.pid, signal.SIGINT)
            try:
                launcher.wait(timeout=30)
            except subprocess.TimeoutExpired:
                receipt["errors"].append("owned launcher cleanup did not finish; do not reuse the fixture")
        save(work / "results.json", receipt)
    return receipt


def compare(paths):
    if len(paths) != 10:
        raise ValueError("all five alternating pairs (ten receipts) are required")
    receipts = [json.loads(Path(path).read_text()) for path in paths]
    if not all(row["completed"] and not row["errors"] for row in receipts):
        return {"pass": False, "errors": ["at least one declared run failed; no samples excluded"]}
    baseline, candidate = receipts[::2], receipts[1::2]
    baseline_hashes = {row["provenance"]["kernel_sha256"] for row in baseline}
    candidate_hashes = {row["provenance"]["kernel_sha256"] for row in candidate}
    if len(baseline_hashes) != 1 or len(candidate_hashes) != 1 or baseline_hashes == candidate_hashes:
        raise ValueError("each role requires one frozen, distinct kernel artifact")
    reference = receipts[0]["provenance"]
    for row in receipts:
        provenance = row["provenance"]
        for key in ("profile", "source_boot_sha256", "source_media_sha256", "media_provenance", "guest_sha256", "runner_sha256"):
            if provenance[key] != reference[key]:
                raise ValueError(f"unmatched provenance {key}")
        if not row.get("cold_recovery", {}).get("pass") or row.get("shutdown", {}).get("quarantined") != 0:
            raise ValueError("missing exact cold recovery and poweroff ownership receipt")
    hardware = []
    for row in candidate:
        latest = {entry["device"]: entry for entry in row["queue_snapshots"]}
        native = [entry for entry in latest.values() if "ahci_depth" in entry]
        if not native or not any(int(entry["hardware_high"]) >= 8 and int(entry["ahci_depth"]) >= 8 and int(entry["ahci_ncq"]) == 1 for entry in native):
            raise ValueError("candidate did not demonstrate negotiated and actual issued QD8")
        for entry in latest.values():
            for key in ("owned", "owned_bytes", "queued", "inflight", "inflight_bytes", "completing", "timeouts"):
                if int(entry[key]) != 0:
                    raise ValueError(f"candidate has nonzero terminal {key}")
            if entry["submitted"] != entry["completed"] or entry["submitted"] != entry["released"]:
                raise ValueError("candidate request ownership is unbalanced")
            if "ahci_depth" in entry:
                for key in ("ahci_active", "hardware_active", "ahci_quarantines", "ahci_offline"):
                    if int(entry[key]) != 0:
                        raise ValueError(f"candidate has nonzero terminal {key}")
        hardware.append(list(latest.values()))
    metrics = {}
    for name in ("qd1", "qd8", "create", "rename"):
        def metric(row):
            data = row["results"][name]["data"]
            return data["mib_per_second"] if name.startswith("qd") else data["iterations"] / data["elapsed_seconds"]
        old, new = [metric(row) for row in baseline], [metric(row) for row in candidate]
        ratio = statistics.median(new) / statistics.median(old)
        gate = PROFILE["qd8_min_ratio"] if name == "qd8" else PROFILE["qd1_min_ratio"]
        metrics[name] = {"baseline_samples": old, "candidate_samples": new, "median_ratio": ratio, "minimum": gate, "pass": ratio >= gate}
    old = [value for row in baseline for value in row["results"]["fsync"]["data"]["latencies_ns"]]
    new = [value for row in candidate for value in row["results"]["fsync"]["data"]["latencies_ns"]]
    old95, new95 = sorted(old)[math.ceil(len(old) * .95) - 1], sorted(new)[math.ceil(len(new) * .95) - 1]
    ratio = new95 / old95
    metrics["fsync"] = {"baseline_samples": old, "candidate_samples": new, "baseline_p95_ns": old95,
                        "candidate_p95_ns": new95, "p95_ratio": ratio, "maximum": PROFILE["fsync_p95_max_ratio"],
                        "pass": ratio <= PROFILE["fsync_p95_max_ratio"]}
    return {"pass": all(value["pass"] for value in metrics.values()), "profile": PROFILE, "metrics": metrics,
            "candidate_terminal_ownership": hardware, "baseline_kernel_sha256": next(iter(baseline_hashes)),
            "candidate_kernel_sha256": next(iter(candidate_hashes)), "receipts": list(map(str, paths)), "errors": []}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    media = sub.add_parser("media", help="snapshot and seed fully materialized private read media; no VM launch")
    media.add_argument("--rootfs-disk", type=Path, required=True)
    media.add_argument("--testprog", type=Path, required=True)
    media.add_argument("--output", type=Path, required=True)
    run = sub.add_parser("run", help="prepare one private single-node fixture, optionally launch")
    run.add_argument("--kernel", type=Path, required=True)
    run.add_argument("--boot-disk", type=Path, required=True)
    run.add_argument("--media-disk", type=Path, required=True)
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--launch", action="store_true")
    run.add_argument("--resume", action="store_true")
    run.add_argument("--timeout-s", type=int, default=900)
    comparison = sub.add_parser("compare", help="evaluate all ten receipts in baseline,candidate alternating order")
    comparison.add_argument("receipts", nargs=10, type=Path)
    comparison.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    for key in ("kernel", "testprog", "boot_disk", "rootfs_disk", "media_disk", "output"):
        if hasattr(args, key):
            setattr(args, key, getattr(args, key).resolve())
    if args.action == "media":
        prepare_media(args)
    elif args.action == "run":
        if args.resume:
            work = args.output
            if not args.launch or not (work / "provenance.json").is_file() or (work / "results.json").exists():
                parser.error("resume requires --launch and a prepared unrun fixture")
        else:
            work = fresh(args.output)
            prepare_run(args, work)
        if args.launch:
            result = launch(args, work)
            print(json.dumps({"completed": result["completed"], "errors": result["errors"], "receipt": str(work / "results.json")}), flush=True)
            return 0 if result["completed"] else 1
    else:
        result = compare(args.receipts)
        save(args.output, result)
        print(json.dumps({"pass": result["pass"], "output": str(args.output)}))
        return 0 if result["pass"] else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
