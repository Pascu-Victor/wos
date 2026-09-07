#!/usr/bin/env python3
"""Check routing partition or short-flap campaign snapshots, failing closed."""
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("directory", type=Path)
parser.add_argument("--flaps", action="store_true", help="check the six short-flap snapshots")
parser.add_argument("--recovery-directory", type=Path,
                    help="use a separately captured recovery on the same uninterrupted VM run")
args = parser.parse_args()
if not __debug__:
    raise RuntimeError("Run without Python -O: validation assertions must remain enabled")
root = args.directory
expected_routes = {
    "baseline0": {2: 2, 3: 2}, "baseline1": {1: 1, 3: 3}, "baseline2": {1: 2, 2: 2},
    "partition0": {2: 2}, "partition1": {1: 1}, "partition2": {},
    "healed0": {2: 2, 3: 2}, "healed1": {1: 1, 3: 3}, "healed2": {1: 2, 2: 2},
}
if args.flaps:
    expected_routes = {label: routes for label, routes in expected_routes.items() if not label.startswith("partition")}
baseline = {}
previous_time = {}
baseline_hop_boots = {}
max_rx = max_spf = 0
for label, expected in expected_routes.items():
    capture_root = args.recovery_directory if label.startswith("healed") and args.recovery_directory else root
    path = capture_root / "snapshots" / label / label / "netdiag.txt"
    lines = path.read_text().splitlines()
    assert lines[-1] == "wki_netdiag_end complete=1", (label, "incomplete snapshot")
    rows = {}
    for line in lines:
        fields = line.split()
        if not fields:
            continue
        rows.setdefault(fields[0], []).append(dict(item.split("=", 1) for item in fields[1:] if "=" in item))
    pool = rows["packet_pool"]
    diag = rows["wki_routing"]
    assert len(pool) == len(diag) == 1, (label, "ambiguous diagnostics")
    pool, diag = pool[0], diag[0]
    projection = {key: int(pool[key]) for key in ("capacity", "active_capacity", "free", "used", "draining", "tx_refused")}
    node = label[-1]
    if label.startswith("baseline"):
        baseline[node] = projection
    else:
        assert projection == baseline[node], (label, "packet accounting changed", projection, baseline[node])
    now = int(diag["now_us"])
    assert now >= previous_time.get(node, 0), (label, "guest clock restarted or captures reordered")
    previous_time[node] = now
    routes = rows.get("wki_route", [])
    actual = {int(row["dst"], 16): int(row["next_hop"], 16) for row in routes}
    assert len(actual) == len(routes), (label, "duplicate destination")
    assert actual == expected, (label, actual, expected)
    assert len({row["generation"] for row in routes}) <= 1, (label, "mixed publication")
    for row in routes:
        assert int(row["generation"]) > 0 and int(row["hop_auth_generation"]) > 0
        assert int(row["lease_expires_us"]) > now, (label, "expired visible route")
        identity = (node, row["dst"])
        if label.startswith("baseline"):
            baseline_hop_boots[identity] = row["hop_boot_epoch"]
        else:
            assert row["hop_boot_epoch"] == baseline_hop_boots[identity], (label, "next hop rebooted between captures")
    assert int(diag["full"]) == 0 and int(diag["malformed"]) == 0, (label, "admission failures")
    max_rx = max(max_rx, int(diag["max_rx_us"]))
    max_spf = max(max_spf, int(diag["max_spf_us"]))
assert max_rx <= 250, ("normal three-node RX budget exceeded", max_rx)
assert max_spf <= 20000, ("normal three-node SPF budget exceeded", max_spf)
print(json.dumps({"snapshots": len(expected_routes), "route_sets": "passed", "coherent_generations": "passed",
                  "lease_deadlines": "passed", "exact_packet_accounting": "passed", "max_rx_us": max_rx, "max_spf_us": max_spf,
                  "separate_recovery_capture": str(args.recovery_directory) if args.recovery_directory else None}))
