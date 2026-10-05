#!/usr/bin/env python3
"""Prepare/run a disposable, rootless IPv4/IPv6 TCP qualification pair.

The caller supplies built kernel/testprog artifacts and existing packaged
disks. Preparation writes only a fresh ktest-data subdirectory. Launch uses
bin/wos-cluster --no-setup and its ordinary exclusive launch guard. It never
stops an existing cluster or modifies the normal/KTEST base disks.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/cluster"))
import node_setup

LEFT_MAC = "52:54:00:00:00:00"
RIGHT_MAC = "52:54:00:01:01:00"
SERVER_V6 = "fe80::5054:ff:fe00:0"
SERVER_TIMEOUT_MS = 180000


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for data in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(data)
    return value.hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def cases(scenario):
    result = []
    for family in (4, 6):
        if scenario == "clean":
            result += [{"scenario": "clean", "family": family, "flows": 1, "sessions": 5,
                        "port": 49000 + family, "bytes": 64 * 1024 * 1024, "payload": 16384},
                       {"scenario": "tiny", "family": family, "flows": 1, "sessions": 20,
                        "port": 49100 + family, "bytes": 64, "payload": 64}]
        elif scenario == "fairness":
            for flows in (2, 4):
                result.append({"scenario": f"fairness{flows}", "family": family, "flows": flows, "sessions": 1,
                               "port": 49200 + family * 10 + flows, "bytes": 16 * 1024 * 1024, "payload": 16384})
        else:
            result.append({"scenario": scenario, "family": family, "flows": 1, "sessions": 1,
                           "port": 49300 + family, "bytes": 8 * 1024 * 1024, "payload": 16384})
    return result


def guest_script(scenario, server):
    # Reports live on the private disk so qualification does not depend on an
    # SSH session, guest serial stdio, or network readiness in PID1's services.
    lines = ["#!/bin/sh", "set -eu", "PATH=/usr/bin:/usr/sbin:/bin:/sbin", "export PATH",
             "mkdir -p /var/tmp", "exec >/var/tmp/wos-tcp-qualification.log 2>&1",
             "trap 'echo TCPFIXTURE-ABORT >/dev/ttyS0; echo TCPREPORT-BEGIN >/dev/ttyS0; cat /var/tmp/wos-tcp-qualification.log >/dev/ttyS0; echo TCPREPORT-END >/dev/ttyS0; sync; poweroff' EXIT",
             f"scope=$(testprog netbench-configure --interface eth0 --ipv4 10.29.0.{1 if server else 2} --ipv6 "
             f"{'fe80::5054:ff:fe00:0' if server else 'fe80::5054:ff:fe01:100'})",
             "ip link set dev eth0 mtu 1500",
             'echo TCPINTERFACE-scope=$scope',
             "echo TCPNET-before", "cat /proc/wki/netdiag | sed -n '/^netdev /p'",
             f"sleep {5 if server else 10}", "echo TCPPOOL-before", "cat /proc/wki/netdiag | sed -n '/^packet_pool /p'", "failed=0"]
    pending = []
    for case in cases(scenario):
        family, flows, sessions, base = case["family"], case["flows"], case["sessions"], case["port"]
        for flow in range(flows):
            port = base + flow * 100
            log = f"/var/tmp/tcp-{port}.json"
            status = log + ".status"
            if server:
                bind = "--family ipv4 --bind 10.29.0.1" if family == 4 else f"--family ipv6 --bind {SERVER_V6} --scope $scope"
                command = f"testprog netbench-server --port {port} --sessions {sessions} --timeout-ms {SERVER_TIMEOUT_MS} {bind}"
                if scenario == "persist":
                    command += " --read-pause-ms 3000 --read-pause-bytes 2097152"
                lines += [f"(set +e; {command} >{log} 2>&1; echo $? >{status}) &"]
                pending.append((case, flow, log, status))
            else:
                host = "10.29.0.1" if family == 4 else SERVER_V6
                scope = " --scope $scope" if family == 6 else ""
                command = (f"testprog netbench-client --host {host} --port {port} --mode stream "
                           f"--total-bytes {case['bytes']} --payload-size {case['payload']} --timeout-ms 60000{scope}")
                if flows > 1:
                    lines += [f"(set +e; {command} >{log} 2>&1; echo $? >{status}) &"]
                    pending.append((case, flow, log, status))
                else:
                    for sample in range(sessions):
                        trace = f"/var/tmp/tcp-trace-{port}.log"
                        if scenario in ("persist", "loss", "reorder", "duplicate", "mixed"):
                            lines += [f"(i=0; while [ $i -lt 450 ]; do echo TCPTICK=$i; cat /proc/wki/netdiag | sed -n '/^tcp_conn /p' | tee /dev/ttyS0; i=$((i+1)); sleep 1; done) >{trace} &",
                                      "observer=$!"]
                        lines += [f"echo TCPCASE scenario={case['scenario']} family={family} flow={flow} sample={sample}",
                                  f"if {command}; then echo TCPSTATUS=0; else echo TCPSTATUS=1; failed=1; fi"]
                        if scenario in ("persist", "loss", "reorder", "duplicate", "mixed"):
                            lines += ["kill $observer || true", "wait $observer || true",
                                      f"cat /proc/wki/netdiag | sed -n '/^tcp_conn /p' >>{trace}",
                                      f"echo TCPTRACE-BEGIN-family={family}", f"cat {trace}", f"echo TCPTRACE-END-family={family}"]
        if not server and flows > 1:
            lines += ["wait"]
            for current, flow, log, status in pending:
                lines += [f"echo TCPCASE scenario={current['scenario']} family={current['family']} flow={flow} sample=0",
                          f"cat {log}", f"value=$(cat {status})", 'echo TCPSTATUS=$value',
                          'if [ "$value" != 0 ]; then failed=1; fi']
            pending.clear()
    if server:
        lines += ["wait"]
        for _, flow, log, status in pending:
            lines += [f"echo TCPSERVER log={log}", f"cat {log}", f"value=$(cat {status})", 'echo TCPSTATUS=$value',
                      'if [ "$value" != 0 ]; then failed=1; fi']
    lines += ["sleep 5", "echo TCPPOOL-after", "cat /proc/wki/netdiag | sed -n '/^packet_pool /p'",
              "echo TCPNET-after", "cat /proc/wki/netdiag | sed -n '/^netdev /p'",
              'echo TCPDONE-failed=$failed',
              "echo TCPREPORT-BEGIN >/dev/ttyS0", "cat /var/tmp/wos-tcp-qualification.log >/dev/ttyS0",
              "echo TCPREPORT-END >/dev/ttyS0", "trap - EXIT", "sync", "poweroff"]
    return "\n".join(lines) + "\n"


MANIFEST = """version=1
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

[service tcp-qualification]
user=root
group=root
supplementary-groups=none
umask=0077
authority-ceiling=65535
no-new-privileges=no
service-privileges=none
exec=/bin/sh
arg=/bin/sh
arg=/usr/libexec/wos-tcp-qualification.sh
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


def command(args, run, name, **kwargs):
    env = dict(os.environ, TMPDIR=str(run))
    with (run / name).open("w") as log:
        subprocess.run(args, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, check=True, **kwargs)


def prepare(args, run):
    boot = run / "boot.qcow2"
    raw = run / "boot.raw"
    command(["qemu-img", "convert", "-f", "qcow2", "-O", "raw", str(args.boot_disk), str(raw)], run, "boot-convert.log")
    config = run / "limine.conf"
    image = f"{raw}@@1048576"
    command(["mcopy", "-o", "-i", image, "::/limine/limine.conf", str(config)], run, "limine-extract.log")
    text = config.read_text()
    assert re.search(r"^\s*cmdline:", text, re.M), "boot image has no kernel command-line entry"
    config.write_text(re.sub(r"^(\s*cmdline:).*", r"\1", text, flags=re.M))
    command(["mcopy", "-o", "-i", image, str(args.kernel), "::/wos"], run, "kernel-inject.log")
    command(["mcopy", "-o", "-i", image, str(config), "::/limine/limine.conf"], run, "limine-inject.log")
    packaged = run / "packaged-wos"
    command(["mcopy", "-o", "-i", image, "::/wos", str(packaged)], run, "kernel-verify.log")
    assert digest(packaged) == digest(args.kernel), "packaged kernel hash mismatch"
    command(["qemu-img", "convert", "-f", "raw", "-O", "qcow2", str(raw), str(boot)], run, "boot-pack.log")
    # Only generated intermediates owned by this runner are removed.
    raw.unlink()
    packaged.unlink()
    manifest = run / "wos-services.conf"
    manifest.write_text(MANIFEST)
    roots = []
    for node in (0, 1):
        disk = run / f"root-base-{node}.qcow2"
        command(["qemu-img", "create", "-f", "qcow2", "-F", "qcow2", "-b", str(args.rootfs_disk), str(disk)],
                run, f"root-create-{node}.log")
        script = run / f"guest-{node}.sh"
        script.write_text(guest_script(args.scenario, node == 0))
        commands = (f"run\nmount /dev/sda1 /\nmkdir-p /usr/libexec\n"
                    f"upload {script} /usr/libexec/wos-tcp-qualification.sh\n"
                    "chmod 0755 /usr/libexec/wos-tcp-qualification.sh\n"
                    f"upload {manifest} /etc/wos-services.conf\nchmod 0644 /etc/wos-services.conf\n"
                    f"upload {args.testprog} /usr/bin/testprog\nchmod 0755 /usr/bin/testprog\numount /\n")
        command(["guestfish", "--rw", "-a", str(disk)], run, f"root-inject-{node}.log", input=commands, text=True)
        roots.append(disk)
    port = 40000 + int.from_bytes(os.urandom(2), "big") % 10000
    backends = [{"type": "socket-mcast", "address": f"239.192.29.{node + 1}", "port": port + node,
                 "localaddr": "127.0.0.1"} for node in (0, 1)]
    zones = [{"id": "GLOBAL", "debug": False, "mtu": 1500, "nic_model": "virtio-net-pci",
              "ivshmem": {"enabled": False}, "bridge": {"ip": None},
              "vm": {"memory": "2G", "cpus": 2, "disk0": str(boot), "overlay_dir": str(run / "overlays"),
                     "usb_hotplug": {"enabled": False}}}]
    for node, backend in enumerate(backends):
        zones.append({"id": node, "name": f"tcp-endpoint-{node}", "node_ids": [node], "nic_queues": 1,
                      "vhost": False, "netdev_driver": "unmanaged", "qemu_netdev": backend, "bridge": {"ip": None},
                      "nodes_config": [{"id": node,
                                        "vm": {"disk1": str(roots[node]), "serial_log": str(run / f"serial-{node}.log"),
                                               "qemu_log": str(run / f"qemu-{node}.log"), "qmp_socket": str(run / f"qmp-{node}.sock")}}]})
    topology = {"zones": zones}
    save(run / "cluster.json", topology)
    import cluster_setup
    cluster_setup.validate_cluster_config(topology)
    # Assert the shared node layout retains the intended MACs/backends.
    for node, info in cluster_setup.collect_unique_nodes(topology).items():
        spec = cluster_setup.cluster_node_spec(node, info, topology)
        assert spec["nics"][0]["mac"] == (LEFT_MAC if node == 0 else RIGHT_MAC), spec
        assert spec["nics"][0]["qemu_netdev"] == backends[node], spec
    save(run / "provenance.json", {"kernel_sha256": digest(args.kernel), "testprog_sha256": digest(args.testprog),
                                   "boot_sha256": digest(boot), "scenario": args.scenario,
                                   "clean_rate_mbps": args.clean_rate_mbps,
                                   "clean_window_bytes": args.clean_window_bytes,
                                   "precise_bridge": args.precise_bridge,
                                   "server_timeout_ms": SERVER_TIMEOUT_MS,
                                   "guest_script_sha256": [digest(run / f"guest-{node}.sh") for node in (0, 1)],
                                   "runner_sha256": digest(Path(__file__)),
                                   "bridge_sha256": digest(ROOT / "scripts/bench/tcp_impairment_bridge.py"),
                                   "source_boot": str(args.boot_disk), "source_rootfs": str(args.rootfs_disk),
                                   "source_boot_sha256": digest(args.boot_disk),
                                   "source_rootfs_sha256": digest(args.rootfs_disk)})
    return backends


def refresh_fixture(args, run):
    for node in (0, 1):
        script = run / f"guest-{node}.sh"
        script.write_text(guest_script(args.scenario, node == 0))
        disk = run / f"root-base-{node}.qcow2"
        command(["guestfish", "--rw", "-a", str(disk)], run, f"fixture-refresh-{node}.log",
                input=f"run\nmount /dev/sda1 /\nupload {script} /usr/libexec/wos-tcp-qualification.sh\nchmod 0755 /usr/libexec/wos-tcp-qualification.sh\numount /\n", text=True)
    proof = json.loads((run / "provenance.json").read_text())
    proof.update(server_timeout_ms=SERVER_TIMEOUT_MS,
                 guest_script_sha256=[digest(run / f"guest-{node}.sh") for node in (0, 1)],
                 runner_sha256=digest(Path(__file__)),
                 bridge_sha256=digest(ROOT / "scripts/bench/tcp_impairment_bridge.py"),
                 source_boot_sha256=digest(args.boot_disk),
                 source_rootfs_sha256=digest(args.rootfs_disk))
    save(run / "provenance.json", proof)


def parse_report(text):
    results, statuses, pools = [], [], []
    current = None
    for line in text.splitlines():
        if line.startswith("TCPCASE "):
            current = dict(re.findall(r"(\w+)=([^ ]+)", line))
        elif line.startswith("{") and current:
            row = json.loads(line)
            row.update(scenario=current["scenario"], flow=int(current["flow"]), sample=int(current["sample"]))
            assert row["family"] == int(current["family"])
            results.append(row)
        elif line.startswith("TCPSTATUS="):
            statuses.append(int(line.split("=", 1)[1]))
        elif line.startswith("packet_pool "):
            pools.append({key: int(value) for key, value in re.findall(r"(\w+)=(\d+)", line)})
    return results, statuses, pools


def persist_trace(text):
    result = []
    for family in (4, 6):
        marker = f"TCPTRACE-BEGIN-family={family}"
        assert marker in text, f"IPv{family}: missing persist trace"
        body = text.split(marker, 1)[1].split(f"TCPTRACE-END-family={family}", 1)[0]
        rows = []
        for line in body.splitlines():
            if line.startswith("tcp_conn ") and f":{49300 + family} " in line.split(" remote=", 1)[-1]:
                rows.append({key: int(value) for key, value in re.findall(r"(\w+)=(\d+)", line)})
        assert rows and any(row.get("snd_wnd") == 0 for row in rows), f"IPv{family}: zero window not observed"
        probes = max(row.get("persist_probes", 0) for row in rows)
        assert probes >= 2, f"IPv{family}: repeated persist probes not observed"
        zeros = [row for row in rows if row.get("snd_wnd") == 0]
        assert all(row.get("timeouts", 0) == 0 for row in zeros), f"IPv{family}: zero window caused congestion timeout"
        reopened = [row for row in rows if row.get("snd_wnd", 0) > 0 and row.get("persist_probes", 0) > 0]
        assert reopened and all(row.get("persist_deadline", 0) == 0 for row in reopened), f"IPv{family}: persist did not stop on reopen"
        result.append({"family": family, "samples": len(rows), "zero_window_samples": len(zeros), "persist_probes": probes,
                       "reopened_samples": len(reopened)})
    return result


def diagnostics(text):
    result = []
    for family in (4, 6):
        marker = f"TCPTRACE-BEGIN-family={family}"
        body = text.split(marker, 1)[1].split(f"TCPTRACE-END-family={family}", 1)[0] if marker in text else text
        rows = [{key: int(value) for key, value in re.findall(r"(\w+)=(\d+)", line)}
                for line in body.splitlines() if line.startswith("tcp_conn ") and
                f":{49300 + family} " in line.split(" remote=", 1)[-1]]
        counters = {key: max((row.get(key, 0) for row in rows), default=0)
                    for key in ("retransmit_packets", "recovery_requests", "timeouts", "sack_blocks", "sack_drops", "persist_probes")}
        result.append({"family": family, "samples": len(rows), "max_counters": counters})
    return result


def launch(args, run, backends):
    receipt = {"format": "wos-tcp-runtime-v1", "completed": False, "errors": [], "results": [], "pool_snapshots": []}
    bridge = launcher = None
    bridge_args = [sys.executable, str(ROOT / "scripts/bench/tcp_impairment_bridge.py"),
                   "--left", f"{backends[0]['address']}:{backends[0]['port']}",
                   "--right", f"{backends[1]['address']}:{backends[1]['port']}",
                   "--left-mac", LEFT_MAC, "--right-mac", RIGHT_MAC, "--seconds", str(args.timeout_s + 30),
                   "--output", str(run / "bridge.json")]
    policies = {"fairness": ["--rate-mbps", "100", "--delay-ms", "5"],
                "loss": ["--drop-every", "17"], "reorder": ["--reorder-every", "11"],
                "duplicate": ["--duplicate-every", "13"],
                "mixed": ["--drop-every", "37", "--reorder-every", "11", "--duplicate-every", "13", "--delay-ms", "2"],
                "persist": ["--rate-mbps", "50"]}
    bridge_args += policies.get(args.scenario, [])
    if args.precise_bridge:
        bridge_args += ["--precise-deadlines"]
    if args.scenario == "clean" and args.clean_rate_mbps:
        bridge_args += ["--rate-mbps", str(args.clean_rate_mbps)]
    if args.scenario == "clean" and args.clean_window_bytes:
        bridge_args += ["--window-cap-bytes", str(args.clean_window_bytes)]
    env = dict(os.environ, TMPDIR=str(run))
    try:
        with (run / "bridge.log").open("w") as bridge_log, (run / "launch.log").open("w") as launch_log:
            bridge = subprocess.Popen(bridge_args, cwd=ROOT, env=env, stdout=bridge_log, stderr=subprocess.STDOUT)
            ready = time.monotonic() + 10
            while "tcp-impairment: READY" not in (run / "bridge.log").read_text():
                assert bridge.poll() is None and time.monotonic() < ready, "bridge failed to become ready"
                time.sleep(0.05)
            launcher = subprocess.Popen([str(ROOT / "bin/wos-cluster"), "--config", str(run / "cluster.json"), "--launch",
                                         "--no-setup", "--live-debug-descriptor", str(run / "live-debug.json")],
                                        cwd=ROOT, env=env, stdout=launch_log, stderr=subprocess.STDOUT, start_new_session=True)
            deadline = time.monotonic() + args.timeout_s
            while launcher.poll() is None:
                assert bridge.poll() is None, "impairment bridge exited during the run"
                assert time.monotonic() < deadline, "rootless cluster run timed out"
                for node in (0, 1):
                    serial_path = run / f"serial-{node}.log"
                    if serial_path.exists() and "TCPDONE-failed=1" in serial_path.read_text(errors="replace"):
                        # Allow the other node's serial receipt to flush before
                        # interrupting this runner's owned process group.
                        time.sleep(3)
                        raise AssertionError(f"node {node}: terminal benchmark failure")
                time.sleep(0.2)
            assert launcher.returncode == 0, "rootless cluster launch/run failed"
        for node in (0, 1):
            destination = run / f"guest-report-{node}.log"
            serial = (run / f"serial-{node}.log").read_text(errors="replace")
            assert serial.count("TCPREPORT-BEGIN") == 1 and serial.count("TCPREPORT-END") == 1, f"node {node}: missing serial receipt"
            text = serial.split("TCPREPORT-BEGIN", 1)[1].split("TCPREPORT-END", 1)[0]
            destination.write_text(text)
            results, statuses, pools = parse_report(text)
            if node == 1:
                receipt["diagnostics"] = diagnostics(text)
            receipt["results"] += results
            receipt["pool_snapshots"].append({"node": node, "before": pools[0] if pools else None, "after": pools[-1] if pools else None})
            assert "TCPDONE-failed=0" in text, f"node {node} failed or did not finish its fixture"
            assert statuses and all(status == 0 for status in statuses), f"node {node}: failed benchmark"
            assert len(pools) == 2, f"node {node}: missing packet-pool snapshots"
            assert pools[0]["used"] == pools[1]["used"], f"node {node}: packet-pool balance changed"
            if node == 1 and args.scenario == "persist":
                receipt["persist"] = persist_trace(text)
        expected = sum(case["flows"] * case["sessions"] for case in cases(args.scenario))
        assert len(receipt["results"]) == expected, "missing client results"
        for node in (0, 1):
            serial = (run / f"serial-{node}.log").read_text(errors="replace")
            assert not re.search(r"KASAN:|KUBSAN:|Kernel panic|PANIC", serial), f"node {node}: kernel diagnostic failure"
        receipt["completed"] = True
    except Exception as error:
        receipt["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        if launcher is not None and launcher.poll() is None:
            # Signal only the launcher/process group created by this runner;
            # its standard cleanup owns exactly this pair of QEMUs.
            os.killpg(launcher.pid, signal.SIGINT)
            try:
                launcher.wait(timeout=30)
            except subprocess.TimeoutExpired:
                receipt["errors"].append("owned launcher did not exit after interrupt")
        if bridge is not None and bridge.poll() is None:
            bridge.terminate()
            bridge.wait(timeout=10)
        # Preserve any terminal guest receipt even when its peer times out or
        # the owned launcher is interrupted after a reported benchmark error.
        receipt["partial_reports"] = []
        for node in (0, 1):
            serial_path = run / f"serial-{node}.log"
            if not serial_path.is_file():
                continue
            serial = serial_path.read_text(errors="replace")
            live = diagnostics(serial)
            receipt.setdefault("live_diagnostics", []).append({"node": node, "connections": live})
            if serial.count("TCPREPORT-BEGIN") == 1 and serial.count("TCPREPORT-END") == 1:
                body = serial.split("TCPREPORT-BEGIN", 1)[1].split("TCPREPORT-END", 1)[0]
                (run / f"guest-report-{node}.log").write_text(body)
                rows, statuses, pools = parse_report(body)
                receipt["partial_reports"].append({"node": node, "results": len(rows), "statuses": statuses,
                                                   "pools": pools, "diagnostics": diagnostics(body)})
        if (run / "bridge.json").exists():
            receipt["bridge"] = json.loads((run / "bridge.json").read_text())
            if receipt["bridge"]["errors"]:
                receipt["completed"] = False
                receipt["errors"].append("impairment bridge failed")
            if args.scenario == "clean" and args.clean_window_bytes and receipt["completed"]:
                counts = receipt["bridge"]["window_cap"]
                for family in (4, 6):
                    if not counts.get(f"ipv{family}_rewritten_windows", 0) or not 0 < counts.get(f"ipv{family}_maximum_effective_window", 0) <= args.clean_window_bytes:
                        receipt["completed"] = False
                        receipt["errors"].append(f"IPv{family}: bounded peer-window profile not observed")
            if args.scenario == "persist" and receipt["completed"]:
                for family in (4, 6):
                    directions = receipt["bridge"]["directions"]
                    if (sum(row.get(f"ipv{family}_zero_window_episodes", 0) for row in directions) < 2 or
                            sum(row.get(f"ipv{family}_window_reopened", 0) for row in directions) < 2):
                        receipt["completed"] = False
                        receipt["errors"].append(f"IPv{family}: repeated window exhaustion/reopen not observed on wire")
        receipt["provenance"] = json.loads((run / "provenance.json").read_text())
        save(run / "results.json", receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=("clean", "fairness", "loss", "reorder", "duplicate", "mixed", "persist"), required=True)
    parser.add_argument("--kernel", type=Path, required=True)
    parser.add_argument("--testprog", type=Path, default=ROOT / "build/modules/testprog/testprog")
    parser.add_argument("--boot-disk", type=Path, default=ROOT / "disk.qcow2")
    parser.add_argument("--rootfs-disk", type=Path, default=ROOT / "mountfs.qcow2")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--launch", action="store_true", help="Launch the prepared pair using bin/wos-cluster --no-setup")
    parser.add_argument("--resume", action="store_true", help="Launch a previously prepared fresh directory without repackaging")
    parser.add_argument("--refresh-fixture", action="store_true", help="Update only scripts in an unrun --resume fixture")
    parser.add_argument("--timeout-s", type=float, default=600)
    parser.add_argument("--clean-rate-mbps", type=float, default=0,
                        help="Equal clean-link capacity calibration for baseline/candidate (0 means unlimited)")
    parser.add_argument("--precise-bridge", action="store_true", help="Use bounded submillisecond bridge deadline service")
    parser.add_argument("--clean-window-bytes", type=int, choices=(0, 131072), default=0,
                        help="Equal clean-link advertised TCP credit calibration (0 or 128KiB)")
    args = parser.parse_args()
    if not 0 <= args.clean_rate_mbps <= 10000:
        parser.error("clean rate must be finite and within 0..10000 Mbps")
    if args.scenario != "clean" and args.clean_window_bytes:
        parser.error("advertised window calibration is only available for clean cases")
    for name in ("kernel", "testprog", "boot_disk", "rootfs_disk"):
        value = getattr(args, name).resolve()
        if not value.is_file():
            parser.error(f"missing {name}: {value}")
        setattr(args, name, value)
    run = args.output.resolve()
    if ROOT / "ktest-data" not in run.parents:
        parser.error("output must be a fresh directory below ktest-data/")
    if args.resume:
        if not args.launch or not (run / "provenance.json").is_file() or (run / "results.json").exists():
            parser.error("resume requires --launch and an unrun, prepared directory")
        proof = json.loads((run / "provenance.json").read_text())
        if proof["scenario"] != args.scenario or proof["kernel_sha256"] != digest(args.kernel) or proof["testprog_sha256"] != digest(args.testprog):
            parser.error("prepared scenario/kernel differs from the requested run")
        if proof.get("clean_rate_mbps", 0) != args.clean_rate_mbps:
            parser.error("prepared clean-link capacity differs from the requested run")
        if proof.get("precise_bridge", False) != args.precise_bridge:
            parser.error("prepared bridge deadline profile differs from the requested run")
        if proof.get("clean_window_bytes", 0) != args.clean_window_bytes:
            parser.error("prepared advertised-window profile differs from the requested run")
        if args.refresh_fixture:
            refresh_fixture(args, run)
        elif proof.get("server_timeout_ms") != SERVER_TIMEOUT_MS:
            parser.error("prepared fixture deadline is stale; use --refresh-fixture for this unrun directory")
        elif proof.get("bridge_sha256") != digest(ROOT / "scripts/bench/tcp_impairment_bridge.py"):
            parser.error("prepared bridge source is stale; use --refresh-fixture for this unrun directory")
        topology = json.loads((run / "cluster.json").read_text())
        backends = [zone["qemu_netdev"] for zone in topology["zones"] if zone["id"] != "GLOBAL"]
    else:
        run.mkdir(parents=True, exist_ok=False)
        backends = prepare(args, run)
    if not args.launch:
        print(f"Prepared private rootless TCP pair: {run / 'cluster.json'}")
        return 0
    result = launch(args, run, backends)
    print(json.dumps({"completed": result["completed"], "errors": result["errors"], "results": str(run / "results.json")}))
    return not result["completed"]


if __name__ == "__main__":
    raise SystemExit(main())
