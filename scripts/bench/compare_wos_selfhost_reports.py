#!/usr/bin/env python3
"""Compare WOS and Linux self-host clone/build timing reports."""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_STEPS = ("clone_sources", "build_wos", "total")
ACCEPTANCE_PROFILE = "checkout-configure"
ACCEPTANCE_STEPS = ("clone_checkout", "configure_wos")
DEFAULT_MAX_WOS_RATIO = 1.25
ACCEPTANCE_MAX_WOS_RATIO = 1.0
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
    parser.add_argument("--wos", required=True, type=Path, help="WOS selfhost-report.tsv")
    parser.add_argument("--linux", required=True, type=Path, help="Linux selfhost-report.tsv")
    parser.add_argument(
        "--max-wos-ratio",
        type=positive_float,
        default=None,
        help="maximum allowed WOS/Linux time ratio for required steps; default 1.25, or 1.0 with --acceptance-profile",
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
        choices=(ACCEPTANCE_PROFILE,),
        help="strict goal check: compare clone_checkout and configure_wos at WOS/Linux ratio <= 1.0",
    )
    parser.add_argument("--json-output", type=Path, help="write comparison evidence as JSON")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.acceptance_profile == ACCEPTANCE_PROFILE:
        if args.wos_detail is None or args.linux_detail is None:
            fail(f"--acceptance-profile {ACCEPTANCE_PROFILE} requires --wos-detail and --linux-detail")
        if args.steps is None:
            args.steps = list(ACCEPTANCE_STEPS)
        if args.max_wos_ratio is None:
            args.max_wos_ratio = ACCEPTANCE_MAX_WOS_RATIO
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
