#!/usr/bin/env python3
"""Check rootless Ethernet bridge bounds, packet classification and fixed gates."""

import importlib.util
import copy
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts/bench" / f"{name}.py")
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


bridge = module("tcp_impairment_bridge")
gates = module("validate_tcp_qualification")
runner = module("run_tcp_qualification")


def packet(version=4, payload=5):
    frame = bytearray(14 + (20 if version == 4 else 40) + 20 + payload)
    frame[12:14] = (0x0800 if version == 4 else 0x86DD).to_bytes(2, "big")
    if version == 4:
        frame[14] = 0x45
        frame[16:18] = (40 + payload).to_bytes(2, "big")
        frame[23] = 6
        frame[46] = 5 << 4
    else:
        frame[14] = 0x60
        frame[18:20] = (20 + payload).to_bytes(2, "big")
        frame[20] = 6
        frame[66] = 5 << 4
    return bytes(frame)


def policy(**changes):
    values = dict(max_packets=4, max_bytes=4096, rate_mbps=0, delay_ms=0,
                  drop_every=0, duplicate_every=0, reorder_every=0, reorder_hold_ms=25)
    values.update(changes)
    return SimpleNamespace(**values)


def main():
    for version in (4, 6):
        frame = packet(version)
        assert bridge.tcp_payload(frame) == (version, 5)
        assert bridge.tcp_payload(frame[:-1]) is None
        assert bridge.tcp_payload(packet(version, 0)) == (version, 0)
        direction = bridge.Direction(policy(drop_every=2))
        for _ in range(4):
            direction.admit(frame, 0)
        assert len(direction.release(0)) == 2 and direction.counts["loss_dropped"] == 2
        direction = bridge.Direction(policy(duplicate_every=1))
        direction.admit(frame, 0)
        assert direction.release(0) == [frame, frame]
        direction = bridge.Direction(policy(reorder_every=2))
        first, held, next_packet = frame + b"a", frame + b"b", frame + b"c"
        for value in (first, held, next_packet):
            direction.admit(value, 0)
        assert direction.release(0) == [first, next_packet, held]
        assert direction.queued_bytes == 0
        direction = bridge.Direction(policy(reorder_every=1))
        direction.admit(frame, 0)
        assert not direction.release(0.01)
        assert direction.release(0.03) == [frame] and direction.queued_bytes == 0
        direction = bridge.Direction(policy(max_packets=2, duplicate_every=1))
        for _ in range(100):
            direction.admit(frame, 0)
        assert len(direction.queue) == 2 and direction.counts["queue_overflow"] > 0
        assert direction.queued_bytes == 2 * len(frame)
        assert len(direction.release(0)) == 2 and direction.queued_bytes == 0
        direction = bridge.Direction(policy(rate_mbps=1, delay_ms=5))
        direction.admit(frame, 0)
        assert not direction.release(0.005)
        assert direction.release(0.006) == [frame]
        assert direction.counts[f"lateness_ipv{version}_tcp_packets"] == 1
        assert direction.counts[f"lateness_ipv{version}_tcp_max_ns"] > 0
        direction = bridge.Direction(policy())
        acknowledgement = bytearray(packet(version, 0))
        offset = 34 if version == 4 else 54
        acknowledgement[offset + 13] = 0x10
        for window in (1000, 0, 0, 1000, 0, 1000):
            acknowledgement[offset + 14:offset + 16] = window.to_bytes(2, "big")
            direction.admit(bytes(acknowledgement), 0)
        assert direction.counts[f"ipv{version}_zero_window_episodes"] == 2
        assert direction.counts[f"ipv{version}_window_reopened"] == 2
        assert direction.counts[f"ipv{version}_zero_window_announcements"] == 3
    assert bridge.tcp_payload(b"malformed") is None
    fragment = bytearray(packet())
    fragment[20:22] = (0x2000).to_bytes(2, "big")
    assert bridge.tcp_payload(fragment) is None
    assert gates.jain([100, 100]) == 1
    assert gates.jain([1, 100]) < 0.90
    assert gates.percentile95(list(range(1, 21))) == 19
    clean = {"completed": True, "errors": [], "results": [],
             "bridge": {"policy": dict(rate_mbps=100, delay_ms=0, drop_every=0, duplicate_every=0,
                                         reorder_every=0, max_packets=1024, max_bytes=8388608,
                                         precise_deadlines=True), "errors": [], "directions": [{}, {}]},
             "provenance": dict(kernel_sha256="kernel", testprog_sha256="testprog", server_timeout_ms=180000,
                                guest_script_sha256=["server", "client"], bridge_sha256="bridge",
                                source_rootfs="rootfs", source_boot="boot", source_rootfs_sha256="rootfs-sha",
                                source_boot_sha256="boot-sha")}
    for family in (4, 6):
        for scenario, count, size in (("clean", 5, 67108864), ("tiny", 20, 64)):
            clean["results"] += [dict(scenario=scenario, family=family, total_bytes=size,
                                      elapsed_us=100, throughput_mib_per_s=10) for _ in range(count)]
    assert all(row["passed"] for row in gates.compare_clean(clean, copy.deepcopy(clean)))
    slower = copy.deepcopy(clean)
    for row in slower["results"]:
        if row["scenario"] == "clean":
            row["throughput_mib_per_s"] = 9.4
        else:
            row["elapsed_us"] = 111
    assert not any(row["passed"] for row in gates.compare_clean(clean, slower))
    pooled, pairs = gates.compare_paired([copy.deepcopy(clean) for _ in range(5)],
                                         [copy.deepcopy(clean) for _ in range(5)])
    assert len(pairs) == 5 and all(row["passed"] for row in pooled)
    assert [row["candidate_samples"] for row in pooled] == [25,100,25,100]
    incomplete = copy.deepcopy(clean)
    incomplete["completed"] = False
    try:
        gates.compare_paired([clean, clean], [clean, incomplete])
    except AssertionError:
        pass
    else:
        raise AssertionError("incomplete paired campaign accepted")
    for field in ("bridge_sha256", "source_rootfs_sha256", "source_boot_sha256"):
        mismatch = copy.deepcopy(clean)
        mismatch["provenance"][field] = "different"
        try:
            gates.compare_clean(clean, mismatch)
        except AssertionError:
            pass
        else:
            raise AssertionError(f"mismatched {field} accepted")
    assert bridge.service_plan(None) == ("select", 0.001)
    assert bridge.service_plan(0.002) == ("select", 0.001)
    action, duration = bridge.service_plan(0.0006)
    assert action == "sleep" and abs(duration - 0.0005) < 1e-12
    action, duration = bridge.service_plan(0.00002)
    assert action == "spin" and duration == 0.00002 and duration <= bridge.FINAL_SPIN_SECONDS
    assert bridge.service_plan(-1) == ("spin", 0)
    clock = SimpleNamespace(now=0.0, sleeps=[])
    def sleep(duration):
        clock.sleeps.append(duration)
        clock.now += duration
    selector = SimpleNamespace(calls=[])
    def select(duration):
        selector.calls.append(duration)
        return []
    selector.select = select
    real_time = bridge.time
    bridge.time = SimpleNamespace(monotonic=lambda: clock.now, sleep=sleep)
    try:
        direction = bridge.Direction(policy(rate_mbps=1))
        direction.enqueue(packet(4), 0)
        assert bridge.wait_events(selector, [direction], True) == []
        assert selector.calls == [0, 0]
        assert len(clock.sleeps) == 1 and 0 < clock.sleeps[0] < 0.001
        # The coarse selector receives only a full millisecond timeout.
        direction.queue[0] = (0.01, direction.queue[0][1], direction.queue[0][2])
        bridge.wait_events(selector, [direction], True)
        assert selector.calls[-1] == 0.001
    finally:
        bridge.time = real_time
    for scenario in ("clean", "fairness", "loss", "reorder", "duplicate", "mixed", "persist"):
        plan = runner.cases(scenario)
        assert {case["family"] for case in plan} == {4, 6}
        ports = [case["port"] + flow * 100 for case in plan for flow in range(case["flows"])]
        assert len(ports) == len(set(ports))
        for server in (False, True):
            script = runner.guest_script(scenario, server)
            subprocess.run(["/bin/sh", "-n"], input=script, text=True, check=True)
            assert "poweroff" in script and "TCPPOOL-before" in script and "TCPPOOL-after" in script
            if not server and scenario in ("loss", "reorder", "duplicate", "mixed", "persist"):
                assert "sleep 1; done" in script and "sleep 0.2" not in script
    report = ('TCPCASE scenario=clean family=4 flow=0 sample=1\n'
              '{"family":4,"elapsed_us":100,"total_bytes":1000}\nTCPSTATUS=0\n'
              'packet_pool capacity=1024 used=8 free=1016\n')
    results, statuses, pools = runner.parse_report(report)
    assert results[0]["scenario"] == "clean" and results[0]["sample"] == 1 and statuses == [0] and pools[0]["used"] == 8
    trace = ""
    for family in (4, 6):
        trace += (f"TCPTRACE-BEGIN-family={family}\n"
                  f"tcp_conn local=0.0.0.0:40000 remote=0.0.0.0:{49300 + family} snd_wnd=0 persist_probes=2 timeouts=0 persist_deadline=100\n"
                  f"tcp_conn local=0.0.0.0:40000 remote=0.0.0.0:{49300 + family} snd_wnd=8192 persist_probes=2 timeouts=0 persist_deadline=0\n"
                  f"TCPTRACE-END-family={family}\n")
    assert all(row["persist_probes"] == 2 for row in runner.persist_trace(trace))
    assert all(row["max_counters"]["persist_probes"] == 2 for row in runner.diagnostics(trace))
    # Validate the actual private service manifest with init's real parser,
    # including its journal dependency and identity/range requirements.
    with tempfile.TemporaryDirectory(prefix="tcp-manifest-", dir=ROOT / "build") as temporary:
        directory = Path(temporary)
        manifest = directory / "manifest.conf"
        manifest.write_text(runner.MANIFEST)
        source = directory / "manifest.cpp"
        source.write_text('''#include "service_manifest.h"
#include <fstream>
#include <iterator>
#include <string>
int main(int argc, char** argv) {
    if (argc != 2) return 1;
    std::ifstream file(argv[1]);
    std::string input(std::istreambuf_iterator<char>{file}, {});
    wos::init::ServiceManifest manifest;
    wos::init::ServiceTopology topology;
    return !wos::init::parse_service_manifest(std::span{input.data(), input.size()}, manifest) ||
           !wos::init::validate_service_manifest(manifest, topology);
}
''')
        binary = directory / "manifest"
        subprocess.run(["/usr/bin/clang++", "-std=c++23", "-I", str(ROOT / "modules/init/src"),
                        str(source), str(ROOT / "modules/init/src/service_manifest.cpp"), "-o", str(binary)],
                       check=True, timeout=30)
        subprocess.run([str(binary), str(manifest)], check=True, timeout=5)
    print("TCP rootless impairment bounds and fixed measurement gates passed")


if __name__ == "__main__":
    main()
