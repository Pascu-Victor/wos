#!/usr/bin/env python3
"""Evaluate fixed TCP clean-link regression and common-bottleneck fairness gates.

Inputs are receipts from run_tcp_qualification.py. Incomplete runs, unmatched
families/byte counts, nonoverlapping fairness runs, or bridge errors fail.
Thresholds are intentionally constants chosen before runtime measurements.
"""

import argparse
import json
import math
from pathlib import Path
import statistics

MIN_CLEAN_RATIO = 0.95
MAX_TINY_P95_RATIO = 1.10
MIN_FAIRNESS = 0.90
MIN_OVERLAP = 0.80


def percentile95(values):
    assert values and all(math.isfinite(value) and value > 0 for value in values)
    ordered = sorted(values)
    return ordered[math.ceil(0.95 * len(ordered)) - 1]


def jain(values):
    assert len(values) >= 2 and all(math.isfinite(value) and value > 0 for value in values)
    return sum(values) ** 2 / (len(values) * sum(value * value for value in values))


def clean_rows(receipt, scenario, family):
    assert receipt["completed"] and not receipt["errors"], "incomplete/failed runtime receipt"
    rows = [row for row in receipt["results"] if row["scenario"] == scenario and row["family"] == family]
    assert rows and all(row["elapsed_us"] > 0 and row["total_bytes"] > 0 for row in rows)
    return rows


def compare_clean(baseline, candidate):
    keys = ("rate_mbps", "delay_ms", "drop_every", "duplicate_every", "reorder_every", "max_packets", "max_bytes")
    before_policy, after_policy = baseline["bridge"]["policy"], candidate["bridge"]["policy"]
    assert all(before_policy[key] == after_policy[key] for key in keys), "different clean-link bridge profiles"
    assert before_policy.get("precise_deadlines", False) == after_policy.get("precise_deadlines", False), "different bridge deadline service"
    assert before_policy.get("window_cap_bytes", 0) == after_policy.get("window_cap_bytes", 0), "different advertised-window calibration"
    if before_policy.get("window_cap_bytes", 0):
        for receipt in (baseline, candidate):
            counts = receipt["bridge"]["window_cap"]
            assert counts["max_flows"] == 128 and counts.get("tracked_flows", 0) <= 128, "different window-cap table bound"
            for family in (4, 6):
                assert counts.get(f"ipv{family}_rewritten_windows", 0), "window cap was not exercised"
                assert 0 < counts.get(f"ipv{family}_maximum_effective_window", 0) <= before_policy["window_cap_bytes"], "advertised credit exceeds profile"
    assert all(before_policy[key] == 0 for key in ("delay_ms", "drop_every", "duplicate_every", "reorder_every")), "clean link has induced impairment"
    assert not baseline["bridge"]["errors"] and not candidate["bridge"]["errors"], "clean bridge failed"
    assert baseline["provenance"]["testprog_sha256"] == candidate["provenance"]["testprog_sha256"], "different benchmark binaries"
    assert baseline["provenance"]["server_timeout_ms"] == candidate["provenance"]["server_timeout_ms"], "different server deadlines"
    assert baseline["provenance"]["guest_script_sha256"] == candidate["provenance"]["guest_script_sha256"], "different guest fixture scripts"
    assert baseline["provenance"].get("bridge_sha256") == candidate["provenance"].get("bridge_sha256"), "different bridge source"
    for key in ("source_rootfs", "source_boot", "source_rootfs_sha256", "source_boot_sha256"):
        assert baseline["provenance"].get(key) == candidate["provenance"].get(key), f"different base image: {key}"
    assert all(not direction.get("queue_overflow", 0) for receipt in (baseline, candidate)
               for direction in receipt["bridge"]["directions"]), "clean bridge queue dropped packets"
    gates = []
    for family in (4, 6):
        before = clean_rows(baseline, "clean", family)
        after = clean_rows(candidate, "clean", family)
        assert len(before) >= 5 and len(after) >= 5, "clean throughput needs five repetitions per family"
        assert len({row["total_bytes"] for row in before + after}) == 1, "different clean byte counts"
        before_median = statistics.median(row["throughput_mib_per_s"] for row in before)
        after_median = statistics.median(row["throughput_mib_per_s"] for row in after)
        ratio = after_median / before_median
        gates.append({"gate": "clean_median_throughput", "family": family, "ratio": ratio,
                      "baseline_median": before_median, "candidate_median": after_median,
                      "baseline_samples": len(before), "candidate_samples": len(after),
                      "minimum": MIN_CLEAN_RATIO, "passed": ratio >= MIN_CLEAN_RATIO})
        before = clean_rows(baseline, "tiny", family)
        after = clean_rows(candidate, "tiny", family)
        assert len(before) >= 20 and len(after) >= 20, "tiny stream p95 needs twenty repetitions per family"
        assert len({row["total_bytes"] for row in before + after}) == 1, "different tiny byte counts"
        before_p95 = percentile95([row["elapsed_us"] for row in before])
        after_p95 = percentile95([row["elapsed_us"] for row in after])
        ratio = after_p95 / before_p95
        gates.append({"gate": "tiny_stream_p95", "family": family, "ratio": ratio,
                      "baseline_p95_us": before_p95, "candidate_p95_us": after_p95,
                      "baseline_samples": len(before), "candidate_samples": len(after),
                      "maximum": MAX_TINY_P95_RATIO, "passed": ratio <= MAX_TINY_P95_RATIO})
    return gates


def compare_paired(baselines, candidates):
    """Pool every declared pair, validating all receipts before aggregation."""
    assert baselines and len(baselines) == len(candidates), "unmatched paired campaign"
    for receipts in (baselines, candidates):
        assert len({row["provenance"]["kernel_sha256"] for row in receipts}) == 1, "campaign changed kernel"
    pairs = []
    for index, (baseline, candidate) in enumerate(zip(baselines, candidates), 1):
        # These comparisons also validate the profile across separate pairs;
        # their performance values are retained, never used to select runs.
        compare_clean(baselines[0], baseline)
        compare_clean(candidates[0], candidate)
        pairs.append({"pair": index, "gates": compare_clean(baseline, candidate)})
    before = dict(baselines[0], results=[row for receipt in baselines for row in receipt["results"]])
    after = dict(candidates[0], results=[row for receipt in candidates for row in receipt["results"]])
    return compare_clean(before, after), pairs


def fairness(receipt):
    assert receipt["completed"] and not receipt["errors"], "incomplete/failed runtime receipt"
    assert receipt["bridge"]["policy"]["rate_mbps"] > 0, "fairness needs a common bounded bottleneck"
    assert not receipt["bridge"]["errors"], "impairment bridge failed"
    gates = []
    for family in (4, 6):
        for flows in (2, 4):
            rows = clean_rows(receipt, f"fairness{flows}", family)
            assert len(rows) == flows and len({row["total_bytes"] for row in rows}) == 1, "unmatched comparable flows"
            start = max(row["started_us"] for row in rows)
            finish = min(row["started_us"] + row["elapsed_us"] for row in rows)
            longest = max(row["elapsed_us"] for row in rows)
            overlap = max(0, finish - start) / longest
            index = jain([row["throughput_mib_per_s"] for row in rows])
            gates.append({"gate": "flow_fairness", "family": family, "flows": flows, "jain": index,
                          "minimum": MIN_FAIRNESS, "overlap_fraction": overlap, "minimum_overlap": MIN_OVERLAP,
                          "passed": index >= MIN_FAIRNESS and overlap >= MIN_OVERLAP})
    return gates


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, nargs="+", required=True)
    parser.add_argument("--candidate", type=Path, nargs="+", required=True)
    parser.add_argument("--fairness", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    gates, errors, pairs = [], [], []
    try:
        baselines = [json.loads(path.read_text()) for path in args.baseline]
        candidates = [json.loads(path.read_text()) for path in args.candidate]
        gates, pairs = compare_paired(baselines, candidates)
        fair_receipt = json.loads(args.fairness.read_text())
        assert fair_receipt["provenance"]["kernel_sha256"] == candidates[0]["provenance"]["kernel_sha256"], "fairness changed kernel"
        gates += fairness(fair_receipt)
    except (AssertionError, KeyError, ValueError, ZeroDivisionError) as error:
        errors.append(str(error))
    result = {"format": "wos-tcp-gates-v1", "gates": gates, "paired_runs": pairs, "errors": errors,
              "baseline_receipts": [str(path) for path in args.baseline],
              "candidate_receipts": [str(path) for path in args.candidate],
              "passed": bool(gates) and not errors and all(gate["passed"] for gate in gates)}
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
    return not result["passed"]


if __name__ == "__main__":
    raise SystemExit(main())
