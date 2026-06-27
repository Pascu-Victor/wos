#!/usr/bin/env python3
"""Compare WOS and Linux self-host clone/build timing reports."""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_STEPS = ("clone_sources", "build_wos", "total")


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
        default=1.25,
        help="maximum allowed WOS/Linux time ratio for required steps; default 1.25",
    )
    parser.add_argument(
        "--steps",
        type=parse_steps,
        default=list(DEFAULT_STEPS),
        help="comma-separated required steps; default clone_sources,build_wos,total",
    )
    parser.add_argument("--json-output", type=Path, help="write comparison evidence as JSON")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    wos = read_report(args.wos)
    linux = read_report(args.linux)
    result = compare_reports(wos, linux, args.steps, args.max_wos_ratio)

    print_text(result)
    if args.json_output is not None:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="ascii")
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
