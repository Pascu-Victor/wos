#!/usr/bin/env python3
"""Compare WOS and Linux self-host clone/build timing reports."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_STEPS = ("clone_sources", "build_wos", "total")
FULL_PROCESS_ACCEPTANCE_PROFILE = "full-process"
CHECKOUT_CONFIGURE_DIAGNOSTIC_PROFILE = "checkout-configure"
CHECKOUT_CONFIGURE_STEPS = ("clone_checkout", "configure_wos")
DEFAULT_MAX_WOS_RATIO = 1.25
FULL_PROCESS_MAX_WOS_RATIO = 1.10
CHECKOUT_CONFIGURE_MAX_WOS_RATIO = 1.0
CLONE_CHECKOUT_EVENTS = (
    "clone:wos_repo",
    "clone:submodule_init",
    "clone:submodules",
)


def fail(message: str) -> None:
    raise SystemExit(f"error: {message}")


def positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected a positive finite number") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("expected a positive finite number")
    return parsed


def parse_steps(value: str) -> list[str]:
    steps = [step.strip() for step in value.split(",") if step.strip()]
    if not steps:
        raise argparse.ArgumentTypeError("expected at least one step name")
    return steps


def read_report(path: Path) -> dict[str, int]:
    if not path.is_file():
        fail(f"missing report: {path}")

    timings: dict[str, int] = {}
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        fields = stripped.split("\t")
        if len(fields) != 2:
            fail(f"{path}:{lineno}: expected '<step>\\t<elapsed-ms>'")

        step, elapsed_text = fields
        if step in timings:
            fail(f"{path}:{lineno}: duplicate step {step!r}")
        try:
            elapsed_ms = int(elapsed_text)
        except ValueError:
            fail(f"{path}:{lineno}: elapsed time is not an integer: {elapsed_text!r}")
        if elapsed_ms < 0:
            fail(f"{path}:{lineno}: elapsed time must be nonnegative")
        timings[step] = elapsed_ms

    if not timings:
        fail(f"{path}: no timing rows found")
    if "total" not in timings:
        timings["total"] = sum(value for step, value in timings.items() if step != "total")
    return timings


def read_detail_report(path: Path) -> dict[str, int]:
    if not path.is_file():
        fail(f"missing detail report: {path}")

    timings: dict[str, int] = {}
    with path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file, delimiter="\t")
        required_fields = {"phase", "label", "elapsed_ms", "status"}
        missing_fields = required_fields - set(reader.fieldnames or ())
        if missing_fields:
            fail(f"{path}: missing detail report fields: {', '.join(sorted(missing_fields))}")

        for lineno, row in enumerate(reader, start=2):
            phase = row["phase"].strip()
            label = row["label"].strip()
            status = row["status"].strip()
            if not phase or not label:
                fail(f"{path}:{lineno}: detail row is missing phase or label")

            step = f"{phase}:{label}"
            if step in timings:
                fail(f"{path}:{lineno}: duplicate detail step {step!r}")
            if status != "ok":
                fail(f"{path}:{lineno}: detail step {step!r} did not pass: {status!r}")

            elapsed_text = row["elapsed_ms"].strip()
            try:
                elapsed_ms = int(elapsed_text)
            except ValueError:
                fail(f"{path}:{lineno}: elapsed time is not an integer: {elapsed_text!r}")
            if elapsed_ms < 0:
                fail(f"{path}:{lineno}: elapsed time must be nonnegative")
            timings[step] = elapsed_ms

    if not timings:
        fail(f"{path}: no detail timing rows found")
    return timings


def derive_clone_checkout(timings: dict[str, int], path: Path) -> int:
    missing = [step for step in CLONE_CHECKOUT_EVENTS if step not in timings]
    if missing:
        fail(f"{path}: missing clone checkout detail steps: {', '.join(missing)}")
    return sum(timings[step] for step in CLONE_CHECKOUT_EVENTS)


def augment_with_detail(report: dict[str, int], detail_path: Path | None) -> dict[str, int]:
    if detail_path is None:
        return report

    timings = dict(report)
    detail = read_detail_report(detail_path)
    timings.update(detail)
    timings["clone_checkout"] = derive_clone_checkout(detail, detail_path)
    return timings


def read_outer_wall_runs(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        fail(f"missing outer-wall run evidence: {path}")

    runs: list[dict[str, Any]] = []
    seen_runs: set[str] = set()
    with path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file, delimiter="\t")
        required_fields = {
            "run",
            "wall_ms",
            "runner_status",
            "evidence_status",
            "accepted",
            "commit",
            "submodules_sha256",
            "repo",
            "mirror_commit",
            "distdir_enabled",
            "distdir_manifest_sha256",
            "timed_out",
        }
        missing_fields = required_fields - set(reader.fieldnames or ())
        if missing_fields:
            fail(f"{path}: missing outer-wall fields: {', '.join(sorted(missing_fields))}")

        for lineno, row in enumerate(reader, start=2):
            run = row["run"].strip()
            if not run:
                fail(f"{path}:{lineno}: run identifier is empty")
            if run in seen_runs:
                fail(f"{path}:{lineno}: duplicate run identifier {run!r}")
            seen_runs.add(run)

            parsed: dict[str, int] = {}
            for field in ("wall_ms", "runner_status", "evidence_status", "accepted", "distdir_enabled", "timed_out"):
                value = row[field].strip()
                try:
                    parsed[field] = int(value)
                except ValueError:
                    fail(f"{path}:{lineno}: {field} is not an integer: {value!r}")
            if parsed["wall_ms"] < 0:
                fail(f"{path}:{lineno}: wall_ms must be nonnegative")
            if parsed["runner_status"] != 0:
                fail(f"{path}:{lineno}: runner did not pass: {parsed['runner_status']}")
            if parsed["evidence_status"] != 0:
                fail(f"{path}:{lineno}: evidence collection did not pass: {parsed['evidence_status']}")
            if parsed["accepted"] != 1:
                fail(f"{path}:{lineno}: run is not accepted")
            if parsed["distdir_enabled"] not in (0, 1):
                fail(f"{path}:{lineno}: distdir_enabled must be 0 or 1")
            if parsed["timed_out"] != 0:
                fail(f"{path}:{lineno}: timed_out must be 0 for acceptance evidence")

            commit = row["commit"].strip()
            submodules_sha256 = row["submodules_sha256"].strip()
            repo = row["repo"].strip()
            mirror_commit = row["mirror_commit"].strip()
            distdir_manifest_sha256 = row["distdir_manifest_sha256"].strip()
            if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
                fail(f"{path}:{lineno}: commit must be a full lowercase 40-hex commit")
            if re.fullmatch(r"[0-9a-f]{64}", submodules_sha256) is None:
                fail(f"{path}:{lineno}: submodules_sha256 must be a lowercase SHA-256 digest")
            if not repo:
                fail(f"{path}:{lineno}: repo provenance must not be empty")
            if mirror_commit and re.fullmatch(r"[0-9a-f]{40}", mirror_commit) is None:
                fail(f"{path}:{lineno}: mirror_commit must be empty or a full lowercase 40-hex commit")
            if parsed["distdir_enabled"] == 1:
                if re.fullmatch(r"[0-9a-f]{64}", distdir_manifest_sha256) is None:
                    fail(f"{path}:{lineno}: enabled distdir requires a lowercase SHA-256 manifest digest")
            elif distdir_manifest_sha256:
                fail(f"{path}:{lineno}: disabled distdir must not have a manifest digest")

            runs.append(
                {
                    "run": run,
                    "wall_ms": parsed["wall_ms"],
                    "commit": commit,
                    "submodules_sha256": submodules_sha256,
                    "repo": repo,
                    "mirror_commit": mirror_commit,
                    "distdir_enabled": parsed["distdir_enabled"],
                    "distdir_manifest_sha256": distdir_manifest_sha256,
                }
            )

    if not runs:
        fail(f"{path}: no outer-wall run rows found")
    return runs


def compare_outer_wall_runs(
    wos_runs: list[dict[str, Any]], linux_runs: list[dict[str, Any]], max_ratio: float
) -> dict[str, Any]:
    combined_runs = [*wos_runs, *linux_runs]
    provenance: dict[str, Any] = {}
    for field in ("commit", "submodules_sha256", "repo", "distdir_enabled", "distdir_manifest_sha256"):
        values = {run[field] for run in combined_runs}
        if len(values) != 1:
            fail(f"full-process comparison requires identical {field} provenance across Linux and WOS runs")
        provenance[field] = next(iter(values))

    mirror_modes = {bool(run["mirror_commit"]) for run in combined_runs}
    if len(mirror_modes) != 1:
        fail("full-process comparison requires Linux and WOS to use the same direct-or-mirror source mode")
    mirrored = next(iter(mirror_modes))
    provenance["source_mode"] = "mirror" if mirrored else "direct"
    if mirrored:
        mirror_commits = {run["mirror_commit"] for run in combined_runs}
        if mirror_commits != {provenance["commit"]}:
            fail("full-process mirror HEAD provenance must equal the checked-out root commit")
        provenance["mirror_commit"] = provenance["commit"]
    else:
        provenance["mirror_commit"] = ""

    if len(linux_runs) == 1:
        paired_linux_runs = linux_runs * len(wos_runs)
    elif len(linux_runs) == len(wos_runs):
        paired_linux_runs = linux_runs
    else:
        fail(
            "full-process comparison requires one Linux baseline run or the same "
            f"number of Linux and WOS runs (got Linux={len(linux_runs)}, WOS={len(wos_runs)})"
        )

    rows: list[dict[str, Any]] = []
    failures: list[str] = []
    for wos_run, linux_run in zip(wos_runs, paired_linux_runs, strict=True):
        wos_ms = wos_run["wall_ms"]
        linux_ms = linux_run["wall_ms"]
        if linux_ms == 0:
            ratio = 1.0 if wos_ms == 0 else math.inf
        else:
            ratio = wos_ms / linux_ms
        passed = ratio <= max_ratio
        step = f"outer_wall:{wos_run['run']}"
        if not passed:
            failures.append(f"{step}: WOS/Linux ratio {ratio:.3f} > {max_ratio:.3f}")
        rows.append(
            {
                "step": step,
                "wos_run": wos_run["run"],
                "linux_run": linux_run["run"],
                "wos_ms": wos_ms,
                "linux_ms": linux_ms,
                "ratio": ratio,
                "pass": passed,
            }
        )

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "max_wos_ratio": max_ratio,
        "metric": "complete_outer_wall_ms",
        "provenance": provenance,
        "steps": rows,
        "missing": [],
        "failures": failures,
        "pass": not failures,
    }


def compare_reports(wos: dict[str, int], linux: dict[str, int], steps: list[str], max_ratio: float) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    missing: list[str] = []
    failures: list[str] = []

    for step in steps:
        if step not in wos:
            missing.append(f"WOS report missing {step}")
            continue
        if step not in linux:
            missing.append(f"Linux report missing {step}")
            continue

        wos_ms = wos[step]
        linux_ms = linux[step]
        if linux_ms == 0:
            ratio = 1.0 if wos_ms == 0 else math.inf
        else:
            ratio = wos_ms / linux_ms
        passed = ratio <= max_ratio
        if not passed:
            failures.append(f"{step}: WOS/Linux ratio {ratio:.3f} > {max_ratio:.3f}")

        rows.append(
            {
                "step": step,
                "wos_ms": wos_ms,
                "linux_ms": linux_ms,
                "ratio": ratio,
                "pass": passed,
            }
        )

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "max_wos_ratio": max_ratio,
        "steps": rows,
        "missing": missing,
        "failures": failures,
        "pass": not missing and not failures,
    }


def print_text(result: dict[str, Any]) -> None:
    print(f"max_wos_ratio\t{result['max_wos_ratio']:.3f}")
    for row in result["steps"]:
        ratio = row["ratio"]
        ratio_text = "inf" if ratio == math.inf else f"{ratio:.3f}"
        status = "PASS" if row["pass"] else "FAIL"
        print(f"{row['step']}\t{row['wos_ms']}\t{row['linux_ms']}\t{ratio_text}\t{status}")
    for message in result["missing"]:
        print(f"MISSING\t{message}", file=sys.stderr)
    for message in result["failures"]:
        print(f"FAIL\t{message}", file=sys.stderr)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wos", type=Path, help="WOS inner selfhost-report.tsv")
    parser.add_argument("--linux", type=Path, help="Linux inner selfhost-report.tsv")
    parser.add_argument("--wos-runs", type=Path, help="WOS runs.tsv containing complete outer wall time")
    parser.add_argument("--linux-runs", type=Path, help="Linux runs.tsv containing complete outer wall time")
    parser.add_argument(
        "--max-wos-ratio",
        type=positive_float,
        default=None,
        help="maximum allowed WOS/Linux ratio; full-process acceptance forbids values above 1.10",
    )
    parser.add_argument(
        "--steps",
        type=parse_steps,
        default=None,
        help="comma-separated required steps; default clone_sources,build_wos,total",
    )
    parser.add_argument("--wos-detail", type=Path, help="WOS selfhost-detail.tsv for detail or derived steps")
    parser.add_argument("--linux-detail", type=Path, help="Linux selfhost-detail.tsv for detail or derived steps")
    parser.add_argument(
        "--acceptance-profile",
        choices=(FULL_PROCESS_ACCEPTANCE_PROFILE, CHECKOUT_CONFIGURE_DIAGNOSTIC_PROFILE),
        help="full-process checks complete outer wall time at <=1.10; checkout-configure is a diagnostic profile",
    )
    parser.add_argument("--json-output", type=Path, help="write comparison evidence as JSON")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.acceptance_profile == FULL_PROCESS_ACCEPTANCE_PROFILE:
        if args.wos_runs is None or args.linux_runs is None:
            fail(
                f"--acceptance-profile {FULL_PROCESS_ACCEPTANCE_PROFILE} requires "
                "--wos-runs and --linux-runs; inner selfhost-report.tsv totals do not include complete outer wall time"
            )
        if args.wos is not None or args.linux is not None or args.wos_detail is not None or args.linux_detail is not None:
            fail("full-process acceptance uses outer runs.tsv evidence, not inner timing reports")
        if args.steps is not None:
            fail("full-process acceptance always compares complete outer wall time; --steps is not allowed")
        if args.max_wos_ratio is None:
            args.max_wos_ratio = FULL_PROCESS_MAX_WOS_RATIO
        elif args.max_wos_ratio > FULL_PROCESS_MAX_WOS_RATIO:
            fail(
                f"full-process acceptance ratio cannot exceed {FULL_PROCESS_MAX_WOS_RATIO:.2f}: "
                f"{args.max_wos_ratio:.3f}"
            )
        result = compare_outer_wall_runs(
            read_outer_wall_runs(args.wos_runs), read_outer_wall_runs(args.linux_runs), args.max_wos_ratio
        )
    else:
        if args.wos is None or args.linux is None:
            fail("inner timing comparison requires --wos and --linux")

        if args.acceptance_profile == CHECKOUT_CONFIGURE_DIAGNOSTIC_PROFILE:
            if args.wos_detail is None or args.linux_detail is None:
                fail(
                    f"--acceptance-profile {CHECKOUT_CONFIGURE_DIAGNOSTIC_PROFILE} requires "
                    "--wos-detail and --linux-detail"
                )
            if args.steps is None:
                args.steps = list(CHECKOUT_CONFIGURE_STEPS)
            if args.max_wos_ratio is None:
                args.max_wos_ratio = CHECKOUT_CONFIGURE_MAX_WOS_RATIO
        else:
            if args.steps is None:
                args.steps = list(DEFAULT_STEPS)
            if args.max_wos_ratio is None:
                args.max_wos_ratio = DEFAULT_MAX_WOS_RATIO

        wos = augment_with_detail(read_report(args.wos), args.wos_detail)
        linux = augment_with_detail(read_report(args.linux), args.linux_detail)
        result = compare_reports(wos, linux, args.steps, args.max_wos_ratio)

    if args.acceptance_profile is not None:
        result["acceptance_profile"] = args.acceptance_profile

    print_text(result)
    if args.json_output is not None:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="ascii")
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
